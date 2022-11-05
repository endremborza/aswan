import sys
import time
import traceback
from dataclasses import dataclass
from typing import Iterable, List, Optional, Type

import requests
from atqo import ActorBase, SchedulerTask
from atqo.utils import partial_cls
from selenium.common.exceptions import WebDriverException
from selenium.webdriver import Chrome
from structlog import get_logger

from .constants import HEADERS, Statuses
from .exceptions import BrokenSessionError, ConnectionError
from .models import CollEvent, RegEvent
from .object_store import ObjectStore
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
    registered_links: List[RegEvent]


@dataclass
class HandlingTask:

    handler: ANY_HANDLER_T
    url: str
    object_store: ObjectStore

    def get_scheduler_task(self) -> SchedulerTask:
        caps = self.handler.get_caps()
        return SchedulerTask(argument=self, requirements=caps)

    def wrap_to_uhr(self, out, status):
        return UrlHandlerResult(
            event=CollEvent(
                handler=self.handler_name,
                url=self.url,
                timestamp=int(time.time()),
                output_file=self.object_store.dump(out) if out is not None else "",
                status=status,
            ),
            registered_links=self.handler.pop_registered_links(),
        )

    @property
    def handler_name(self):
        return self.handler.name


class ConnectionSession(ActorBase):
    def __init__(
        self,
        is_browser=False,
        headless=True,
        eager=False,
        proxy_cls: Type[ProxyBase] = DEFAULT_PROXY,
    ):
        self.is_browser = is_browser
        self.eager = eager
        self._proxy = proxy_cls()

        self.session = (
            BrowserSession(headless, self.eager)
            if self.is_browser
            else RequestSession()
        )
        self._initiated_handlers = set()
        self._broken_handlers = set()
        self._num_queries = 0
        self.session.start(self._proxy)

    def consume(self, task: HandlingTask) -> UrlHandlerResult:
        handler_name = task.handler_name
        if (handler_name in self._broken_handlers) or (
            task.handler.restart_session_after < self._num_queries
        ):
            self._restart(new_proxy=False)

        if handler_name not in self._initiated_handlers:
            self._initiate_handler(task.handler)

        cached_resp = task.handler.load_cache(task.url)
        if cached_resp is not None:
            status = (
                Statuses.PERSISTENT_CACHED
                if task.handler.process_indefinitely
                else Statuses.CACHE_LOADED
            )
            return task.wrap_to_uhr(cached_resp, status)

        task.handler.set_url(task.url)
        time.sleep(task.handler.get_sleep_time())
        uh_result = self._get_uh_result(task)
        if uh_result.event.status == Statuses.SESSION_BROKEN:
            self._broken_handlers.add(handler_name)
        self._num_queries += 1
        return uh_result

    def stop(self):
        self.session.stop()

    def get_parsed_response(self, url, handler=RequestSoupHandler(), params=None):
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
            logger.warning("Missed try", error=str(content), url=url, attempt=attempt)
            if handler.is_session_broken(content):
                raise BrokenSessionError(f"error: {content}")
            time.sleep(handler.get_retry_sleep_time())
        else:
            raise ConnectionError(f"request resulted in error with status {content}")
        return handler.parse(handler.pre_parse(content))

    def _restart(self, new_proxy=True):
        self.session.stop()
        if new_proxy:
            self._proxy.set_new_host()
        self._initiated_handlers = set()
        self._broken_handlers = set()
        self.session.start(self._proxy)
        self._num_queries = 0

    def _get_uh_result(
        self,
        task: HandlingTask,
    ) -> UrlHandlerResult:
        try:
            out = self.get_parsed_response(task.url, task.handler)
            if task.handler.process_indefinitely:
                status = Statuses.PERSISTENT_PROCESSED
            else:
                status = Statuses.PROCESSED
        except Exception as e:
            out = _parse_exception(e)
            status = EXCEPTION_STATUSES.get(type(e), DEFAULT_EXCEPTION_STATUS)
            _info = {**out, "proxy": self._proxy.host, "status": status}
            logger.warning("Gave up", handler=task.handler_name, url=task.url, **_info)

        return task.wrap_to_uhr(out, status)

    def _initiate_handler(self, handler: ANY_HANDLER_T):
        for _ in range(handler.initiation_retries):
            try:
                handler.start_session(self.session.driver)
                self._initiated_handlers.add(handler.name)
                return True
            except Exception as e:
                logger.warning(
                    "Failed initiating handler",
                    handler_name=handler.name,
                    **_parse_exception(e),
                )
                # TODO: might be overkill this
                self._restart()
                time.sleep(handler.wait_on_initiation_fail)


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
        self.driver.headers.update(HEADERS)  # TODO custom headers
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


def get_actor_dict(handlers: Iterable[ANY_HANDLER_T]):
    out = {}
    for handler in handlers:
        caps = handler.get_caps()
        full_kwargs = dict(proxy_cls=handler.proxy_cls)
        for cap in caps:
            full_kwargs.update(cap_to_kwarg.get(cap, {}))
        if (
            full_kwargs.get("is_browser")
            and full_kwargs.get("headless", True)
            and handler.proxy.needs_auth
        ):
            raise RuntimeError("can't have auth (extension) in headless browser")
        actor = partial_cls(ConnectionSession, **full_kwargs)
        out[caps] = actor
    return out


def _parse_exception(e):
    tbl = [tb.strip().split("\n") for tb in traceback.format_tb(e.__traceback__)]
    return {"e_type": type(e).__name__, "e": str(e), "tb": tbl}
