import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import requests
from atqo import ActorBase, ActorSlot

from .constants import (
    HEADERS,
    RELAY_PORT,
    Statuses,
)
from .depot import AswanDepot
from .exceptions import AswanConnectionError as ConnectionError, BrokenSessionError
from .models import CollEvent
from .security import DEFAULT_PROXY, ProxyBase
from .url_handler import (
    ANY_HANDLER_T,
    BrowserHandler,
    RelayHandler,
    RequestSoupHandler,
)
from .utils import add_url_params

logger = logging.getLogger(__name__)

EXCEPTION_STATUSES = {
    ConnectionError: Statuses.CONNECTION_ERROR,
    BrokenSessionError: Statuses.SESSION_BROKEN,
}
DEFAULT_EXCEPTION_STATUS = Statuses.PARSING_ERROR


@dataclass
class HandlingTask:
    handler: ANY_HANDLER_T
    url: str


class ConnectionSession(ActorBase):
    def __init__(
        self,
        depot_path: Path | None = None,
        is_browser=False,
        headless=True,
        eager=False,
        proxy_cls: type[ProxyBase] = DEFAULT_PROXY,
        is_relay: bool = False,
        relay_port: int = RELAY_PORT,
    ):
        self.is_browser = is_browser
        self.eager = eager
        self._proxy = proxy_cls()
        # Workers only touch the object store (content-addressed, no SQLite
        # contention) and the events dir (also content-addressed filenames).
        # SQLite is owned exclusively by the main process; events ride back
        # through the scheduler result for the main process to integrate.
        if depot_path is not None:
            depot = AswanDepot(depot_path.name, depot_path.parent)
            self.events_dir = depot.current.events
            self.events_dir.mkdir(parents=True, exist_ok=True)
            self.store = depot.object_store
        else:
            self.events_dir = None
            self.store = None

        if is_relay:
            from ._relay_session import RelaySession

            self.session = RelaySession(relay_port)
        elif self.is_browser:
            self.session = BrowserSession(headless, self.eager)
        else:
            self.session = RequestSession()

        self._initiated_handlers = set()
        self._broken_handlers = set()
        self._handler_cache: dict[str, ANY_HANDLER_T] = {}
        self._num_queries = 0
        self.session.start(self._proxy)

    def consume(self, task: HandlingTask):
        # MultiProcAPI pickles task per dispatch, so handler state would reset
        # every call without this cache.
        handler = self._handler_cache.setdefault(task.handler.name, task.handler)
        handler_name = handler.name
        if (handler_name in self._broken_handlers) or (
            handler.restart_session_after < self._num_queries
        ):
            self._restart(new_proxy=False)

        if handler_name not in self._initiated_handlers:
            self._initiate_handler(handler)

        try:
            cached_resp = handler.load_cache(task.url)
        except Exception as e:
            logger.warning("error during cache loading %s %s", type(e), e)
            cached_resp = None
        if cached_resp is not None:
            status = Statuses.CACHE_LOADED
            out = cached_resp
        else:
            handler.set_url(task.url)
            time.sleep(handler.get_sleep_time())
            out, status = self._get_out_and_status(handler, task.url)
            if status == Statuses.SESSION_BROKEN:
                self._broken_handlers.add(handler_name)
            self._num_queries += 1
        return self.proc_result(handler, task.url, out, status)

    def stop(self):
        self.session.stop()

    def get_parsed_response(
        self, url, handler: ANY_HANDLER_T | None = None, params: dict | None = None
    ):
        if handler is None:
            handler = RequestSoupHandler()
        if params:
            url = add_url_params(url, params)
        for attempt in range(handler.max_retries):
            try:
                content = self.session.get_response_content(handler, url)
                if not isinstance(content, int):
                    # int is non 200 response code
                    break
            except Exception as e:
                content = e
            self._log_miss("Failed Try", content, handler, attempt, "RETRY", url)
            if handler.is_session_broken(content):
                raise BrokenSessionError(f"error: {content}".split("\n")[0])
            time.sleep(handler.get_retry_sleep_time())
        else:
            raise ConnectionError(f"request resulted in error with status {content}")
        return handler.parse(handler.pre_parse(content))

    def proc_result(self, handler: ANY_HANDLER_T, url: str, out, status: str) -> list:
        event = CollEvent(
            handler=handler.name,
            url=url,
            timestamp=int(time.time()),
            output_file=self.store.dump(out) if out is not None else "",
            status=status,
        )
        events = [event, *handler.pop_registered_links()]
        if self.events_dir is not None:
            for e in events:
                e.dump(self.events_dir)
        return events

    def _restart(self, new_proxy=True):
        self.session.stop()
        if new_proxy:
            self._proxy.set_new_host()
        self._initiated_handlers = set()
        self._broken_handlers = set()
        self.session.start(self._proxy)
        self._num_queries = 0

    def _get_out_and_status(self, handler: ANY_HANDLER_T, url: str) -> tuple:
        try:
            out = self.get_parsed_response(url, handler)
            status = Statuses.PROCESSED
        except Exception as e:
            out = _parse_exception(e)
            status = EXCEPTION_STATUSES.get(type(e), DEFAULT_EXCEPTION_STATUS)
            self._log_miss("Gave Up", e, handler, handler.max_retries, status, url)
        return out, status

    def _initiate_handler(self, handler: ANY_HANDLER_T):
        for att in range(handler.initiation_retries):
            try:
                handler.start_session(self.session.driver)
                self._initiated_handlers.add(handler.name)
                return True
            except Exception as e:
                self._log_miss("Failed initiating handler", e, handler, att, "PRE", "")
                self._restart()
                time.sleep(handler.wait_on_initiation_fail)

    def _log_miss(self, msg, content, handler: ANY_HANDLER_T, attempt, status, url):
        out = _parse_exception(content)
        _info = out | {"proxy": self._proxy.host, "status": status}
        logger.warning(
            "%s handler=%s url=%s attempt=%s info=%s",
            msg,
            handler.name,
            url,
            attempt,
            _info,
        )


class BrowserSession:
    def __init__(self, headless: bool, eager: bool, show_images: bool = False):
        self._headless = headless
        self._eager = eager
        self._show_images = show_images
        self.driver = None

    def start(self, proxy: ProxyBase):
        from selenium.webdriver import Chrome

        chrome_options = proxy.get_chrome_options()
        if sys.platform == "linux":
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--no-sandbox")
        if self._headless:
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--headless")
        if self._eager:  # pragma: no cover
            chrome_options.page_load_strategy = "eager"
        if not self._show_images:
            prefs = {"profile.managed_default_content_settings.images": 2}
            chrome_options.add_experimental_option("prefs", prefs)

        logger.info("launching browser: %s", chrome_options.arguments)
        self.driver = Chrome(options=chrome_options)
        logger.info("browser running")

    def stop(self):
        try:
            logger.info("stopping browser")
            # quit(), not close(): close() only drops the active window and
            # leaves chromedriver + chrome running, leaking processes that pile
            # up and wedge later browser launches.
            self.driver.quit()
        except Exception as e:  # pragma: no cover
            logger.warning("could not stop browser: %s", e)

    def get_response_content(self, handler: ANY_HANDLER_T, url: str):
        self.driver.get(url)
        out = handler.handle_driver(self.driver)
        if out is not None:
            return out
        return self.driver.page_source.encode("utf-8")


class RequestSession:
    def __init__(self):
        self.driver: requests.Session | None = None

    def start(self, proxy: ProxyBase):
        self.driver = requests.Session()
        self.driver.headers.update(HEADERS)
        self.driver.proxies.update(proxy.get_requests_dict())

    def stop(self):
        pass

    def get_response_content(self, handler: ANY_HANDLER_T, url: str):
        handler.handle_driver(self.driver)
        resp = self.driver.get(url)
        if resp.ok:
            return resp.content
        return resp.status_code


def _parse_exception(e):
    return {"e_type": type(e).__name__, "e_msg": str(e).split("\n")[0]}


def get_session_slot(handler: ANY_HANDLER_T, depot_path: Path) -> ActorSlot:
    # One ConnectionSession class backs every handler; atqo's ActorSlot carries
    # the per-handler init kwargs and resource profile. The slot's partial
    # pickles cleanly across a MultiProcAPI spawn (module-level class + plain
    # kwargs), so no dynamic-subclass machinery is needed.
    kwargs: dict = {"proxy_cls": handler.proxy_cls, "depot_path": depot_path}
    if isinstance(handler, BrowserHandler):
        kwargs["is_browser"] = True
        kwargs["headless"] = handler.headless
        kwargs["eager"] = handler.eager
    if isinstance(handler, RelayHandler):
        kwargs["is_relay"] = True
        kwargs["relay_port"] = handler.listen_port
    if (
        kwargs.get("is_browser")
        and kwargs.get("headless", True)
        and handler.proxy.needs_auth
    ):
        raise RuntimeError("can't have auth (extension) in headless browser")
    return ActorSlot(
        ConnectionSession,
        name=handler.name,
        kwargs=kwargs,
        requirements=handler.get_requirements(),
    )
