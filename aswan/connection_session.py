import sys
import time
import traceback
from dataclasses import dataclass, field
from itertools import product
from typing import Dict, Iterable, List, Optional, Type

import requests
from atqo import ActorBase, CapabilitySet, SchedulerTask
from atqo.utils import partial_cls
from selenium.common.exceptions import WebDriverException
from selenium.webdriver import Chrome
from structlog import get_logger

from .constants import HEADERS, Statuses
from .exceptions import BrokenSessionError, ConnectionError
from .object_store import ObjectStore
from .resources import Caps
from .security import DEFAULT_PROXY, ProxyBase, ProxyData
from .url_handler import (
    ANY_HANDLER_T,
    BrowserHandler,
    RegisteredLink,
    RequestSoupHandler,
)
from .utils import add_url_params

logger = get_logger()

EXCEPTION_STATUSES = {
    ConnectionError: Statuses.CONNECTION_ERROR,
    BrokenSessionError: Statuses.SESSION_BROKEN,
}
DEFAULT_EXCEPTION_STATUS = Statuses.PARSING_ERROR


@dataclass
class UrlHandlerResult:
    handler_name: str
    url: str
    status: str
    timestamp: int
    output_file: Optional[str]
    expiration_seconds: Optional[int]
    registered_links: List["RegisteredLink"] = field(default_factory=list)


@dataclass
class HandlingTask:

    handler: ANY_HANDLER_T
    url: str
    object_store: ObjectStore

    def get_scheduler_task(self, proxy_dic: Dict[str, ProxyData]) -> SchedulerTask:
        caps = [proxy_dic[self.handler.proxy_kind].cap]
        if isinstance(self.handler, BrowserHandler):
            if not self.handler.headless:
                caps.append(Caps.display)
            if self.handler.eager:
                caps.append(Caps.eager_browser)
            else:
                caps.append(Caps.normal_browser)

        return SchedulerTask(argument=self, requirements=caps)

    @property
    def handler_name(self):
        return self.handler.name


class ConnectionSession(ActorBase):
    def __init__(
        self,
        is_browser=False,
        headless=True,
        eager=False,
        proxy_kls: Type[ProxyBase] = DEFAULT_PROXY,
    ):
        self.is_browser = is_browser
        self.eager = eager
        self._proxy_kls = proxy_kls()
        self.proxy_host = self._proxy_kls.get_new_host()

        self.session = (
            BrowserSession(headless, self.eager)
            if self.is_browser
            else RequestSession()
        )
        self._initiated_handlers = set()
        self._broken_handlers = set()
        self._num_queries = 0
        self.session.start(self._proxy_kls, self.proxy_host)

    def consume(self, task: HandlingTask) -> UrlHandlerResult:
        handler_name = task.handler_name
        if (handler_name in self._broken_handlers) or (
            task.handler.restart_session_after < self._num_queries
        ):
            self._restart(new_proxy=False)

        if handler_name not in self._initiated_handlers:
            self._initiate_handler(task.handler)

        task.handler.set_url(task.url)
        task.handler.reset_expiration()
        time.sleep(task.handler.get_sleep_time())
        uh_result = self._get_uh_result(task)
        if uh_result.status == Statuses.SESSION_BROKEN:
            self._broken_handlers.add(handler_name)
        self._num_queries += 1
        return uh_result

    def stop(self):
        self.session.stop()

    def get_parsed_response(self, url, handler=RequestSoupHandler(), params=None):
        """returns json serializable or dies trying"""
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
            self.proxy_host = self._proxy_kls.get_new_host()
        self._initiated_handlers = set()
        self._broken_handlers = set()
        self.session.start(self._proxy_kls, self.proxy_host)
        self._num_queries = 0

    def _get_uh_result(
        self,
        task: HandlingTask,
    ) -> UrlHandlerResult:
        try:
            out = self.get_parsed_response(task.url, task.handler)
            status = Statuses.PROCESSED
            outfile = task.object_store.dump_json(out) if out is not None else out
        except Exception as e:
            out = _parse_exception(e)
            status = EXCEPTION_STATUSES.get(type(e), DEFAULT_EXCEPTION_STATUS)
            outfile = None
            _info = {**out, "proxy": self.proxy_host, "status": status}
            logger.warning("Gave up", handler=task.handler_name, url=task.url, **_info)

        return UrlHandlerResult(
            handler_name=task.handler_name,
            url=task.url,
            timestamp=int(time.time()),
            output_file=outfile,
            status=status,
            expiration_seconds=task.handler.expiration_seconds,
            registered_links=task.handler.pop_registered_links(),
        )

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
    def __init__(self, headless: bool, eager: bool):
        self._headless = headless
        self._eager = eager
        self.driver: Optional[Chrome] = None

    def start(self, proxy_kls: ProxyBase, proxy_host: str):
        chrome_options = proxy_kls.chrome_optins_from_host(proxy_host)
        if sys.platform == "linux":
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--no-sandbox")
        if self._headless:
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--headless")
        if self._eager:
            chrome_options.page_load_strategy = "eager"
        logger.info(f"launching browser: {chrome_options.arguments}")
        self.driver = Chrome(options=chrome_options)
        logger.info("browser running")

    def stop(self):
        try:
            self.driver.close()
        except WebDriverException:
            logger.warning("could not stop browser")

    def get_response_content(self, handler: ANY_HANDLER_T, url: str):
        self.driver.get(url)
        out = handler.handle_driver(self.driver)
        if out is not None:
            return out
        return self.driver.page_source


class RequestSession:
    def __init__(self):
        self.driver: Optional[requests.Session] = None

    def start(self, proxy_kls: ProxyBase, proxy_host: str):
        self.driver = requests.Session()
        self.driver.headers.update(HEADERS)
        self.driver.proxies.update(proxy_kls.rdict_from_host(proxy_host))

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


def get_actor_dict(all_proxy_data: Iterable[ProxyData]):

    browsets = [[Caps.eager_browser], [Caps.normal_browser]]
    base_capsets = [[Caps.simple]] + browsets + [[Caps.display, *bs] for bs in browsets]
    out = {}
    for proxy_data, capset in product(all_proxy_data, base_capsets):
        full_kwargs = dict(proxy_kls=proxy_data.kls)
        for cap in capset:
            full_kwargs.update(cap_to_kwarg.get(cap, {}))
        actor = partial_cls(ConnectionSession, **full_kwargs)
        out[CapabilitySet([proxy_data.cap, *capset])] = actor

    return out


def _parse_exception(e):
    tbl = [tb.strip().split("\n") for tb in traceback.format_tb(e.__traceback__)]
    return {"e_type": type(e).__name__, "e": str(e), "tb": tbl}
