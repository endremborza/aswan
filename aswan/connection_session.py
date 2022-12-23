import sys
import time
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, Iterable, Optional

import requests
from atqo import ActorBase, SchedulerTask
from selenium.common.exceptions import WebDriverException
from selenium.webdriver import Chrome
from structlog import get_logger

from .constants import HEADERS, Statuses
from .depot import AswanDepot
from .exceptions import BrokenSessionError, ConnectionError
from .models import CollEvent, RegEvent
from .resources import Caps
from .security import DEFAULT_PROXY, ProxyBase
from .url_handler import ANY_HANDLER_T, RequestSoupHandler
from .utils import add_url_params

logger = get_logger()

EXCEPTION_STATUSES = {
    ConnectionError: Statuses.CONNECTION_ERROR,
    BrokenSessionError: Statuses.SESSION_BROKEN,
}
DEFAULT_EXCEPTION_STATUS = Statuses.PARSING_ERROR


@dataclass
class UrlHandlerResult:
    event: CollEvent
    registered_links: list[RegEvent]


@dataclass
class HandlingTask:

    handler: ANY_HANDLER_T
    url: str

    def get_scheduler_task(self) -> SchedulerTask:
        caps = self.handler.get_caps()
        return SchedulerTask(argument=self, requirements=caps)


class ConnectionSession(ActorBase):
    def __init__(
        self,
        depot_path: Optional[Path] = None,
        is_browser=False,
        headless=True,
        eager=False,
        proxy_cls: type[ProxyBase] = DEFAULT_PROXY,
    ):
        self.is_browser = is_browser
        self.eager = eager
        self._proxy = proxy_cls()
        if depot_path is not None:
            depot = AswanDepot(depot_path.name, depot_path.parent)
            self.current = depot.current.setup()
            self.store = depot.object_store
        else:
            self.current = None
            self.store = None

        self.session = (
            BrowserSession(headless, self.eager)
            if self.is_browser
            else RequestSession()
        )
        self._initiated_handlers = set()
        self._broken_handlers = set()
        self._num_queries = 0
        self.session.start(self._proxy)

    def consume(self, task: HandlingTask):
        handler_name = task.handler.name
        if (handler_name in self._broken_handlers) or (
            task.handler.restart_session_after < self._num_queries
        ):
            self._restart(new_proxy=False)

        if handler_name not in self._initiated_handlers:
            self._initiate_handler(task.handler)

        try:
            cached_resp = task.handler.load_cache(task.url)
        except Exception as e:
            logger.warning("error during cache loading", e=e, e_type=type(e))
            cached_resp = None
        if cached_resp is not None:
            status = (
                Statuses.PERSISTENT_CACHED
                if task.handler.process_indefinitely
                else Statuses.CACHE_LOADED
            )
            out = cached_resp
        else:
            task.handler.set_url(task.url)
            time.sleep(task.handler.get_sleep_time())
            out, status = self._get_out_and_status(task)
            if status == Statuses.SESSION_BROKEN:
                self._broken_handlers.add(handler_name)
            self._num_queries += 1
        self.proc_result(task, out, status)

    def stop(self):
        self.session.stop()

    def get_parsed_response(
        self, url, handler=RequestSoupHandler(), params: Optional[dict] = None
    ):
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

    def proc_result(self, task: HandlingTask, out: Any, status: str):
        event = CollEvent(
            handler=task.handler.name,
            url=task.url,
            timestamp=int(time.time()),
            output_file=self.store.dump(out) if out is not None else "",
            status=status,
        )
        self.current.integrate_events([event, *task.handler.pop_registered_links()])

    def _restart(self, new_proxy=True):
        self.session.stop()
        if new_proxy:
            self._proxy.set_new_host()
        self._initiated_handlers = set()
        self._broken_handlers = set()
        self.session.start(self._proxy)
        self._num_queries = 0

    def _get_out_and_status(self, task: HandlingTask) -> tuple[Any, str]:
        try:
            out = self.get_parsed_response(task.url, task.handler)
            if task.handler.process_indefinitely:
                status = Statuses.PERSISTENT_PROCESSED
            else:
                status = Statuses.PROCESSED
        except Exception as e:
            out = _parse_exception(e)
            status = EXCEPTION_STATUSES.get(type(e), DEFAULT_EXCEPTION_STATUS)
            _h = task.handler
            self._log_miss("Gave Up", e, _h, _h.max_retries, status, task.url)
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
        logger.warning(msg, handler=handler.name, url=url, attempt=attempt, **_info)


class BrowserSession:
    def __init__(self, headless: bool, eager: bool, show_images: bool = False):
        self._headless = headless
        self._eager = eager
        self._show_images = show_images
        self.driver: Optional[Chrome] = None

    def start(self, proxy: ProxyBase):
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

        logger.info(f"launching browser: {chrome_options.arguments}")
        self.driver = Chrome(options=chrome_options)
        logger.info("browser running")

    def stop(self):
        try:
            self.driver.close()
        except WebDriverException:  # pragma: no cover
            logger.warning("could not stop browser")

    def get_response_content(self, handler: ANY_HANDLER_T, url: str):
        self.driver.get(url)
        out = handler.handle_driver(self.driver)
        if out is not None:
            return out
        return self.driver.page_source.encode("utf-8")


class RequestSession:
    def __init__(self):
        self.driver: Optional[requests.Session] = None

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


cap_to_kwarg = {
    Caps.display: dict(headless=False),
    Caps.normal_browser: dict(is_browser=True),
    Caps.eager_browser: dict(is_browser=True, eager=True),
}


def get_actor_items(handlers: Iterable[ANY_HANDLER_T], depot_path: Path):
    for handler in handlers:
        caps = handler.get_caps()
        full_kwargs = dict(proxy_cls=handler.proxy_cls, depot_path=depot_path)
        for cap in caps:
            full_kwargs.update(cap_to_kwarg.get(cap, {}))
        if (
            full_kwargs.get("is_browser")
            and full_kwargs.get("headless", True)
            and handler.proxy.needs_auth
        ):
            raise RuntimeError("can't have auth (extension) in headless browser")
        yield caps, partial(ConnectionSession, **full_kwargs)


def _parse_exception(e):
    # tbl = [tb.strip().split("\n") for tb in traceback.format_tb(e.__traceback__)]
    return {"e_type": type(e).__name__, "e_msg": str(e).split("\n")[0]}  # , "tb": tbl}
