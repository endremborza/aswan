import json
import sys
import time
import traceback
from dataclasses import dataclass, field
from functools import partial
from itertools import product
from json.decoder import JSONDecodeError
from typing import Dict, Iterable, List, Optional

import requests
from atqo import ActorBase, CapabilitySet, SchedulerTask
from bs4 import BeautifulSoup
from selenium.common.exceptions import WebDriverException
from selenium.webdriver import Chrome
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from structlog import get_logger

from .constants import HEADERS, Statuses
from .exceptions import BrokenSessionError, RequestError
from .object_store import ObjectStoreBase
from .resources import Caps
from .security import DEFAULT_PROXY, ProxyBase, ProxyData
from .url_handler import RegisteredLink, UrlHandler
from .utils import add_url_params

logger = get_logger()

EXCEPTION_STATUSES = {
    RequestError: Statuses.CONNECTION_ERROR,
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

    handler: UrlHandler
    url: str
    object_store: ObjectStoreBase

    def get_scheduler_task(self, proxy_dic) -> SchedulerTask:
        return SchedulerTask(
            argument=self,
            requirements=get_caps(self.handler, proxy_dic),
        )

    @property
    def handler_name(self):
        return self.handler.name


def get_caps(handler: UrlHandler, proxy_dic: Dict[str, ProxyData]):

    try:
        p_data = proxy_dic[handler.proxy_kind]
    except KeyError:
        logger.warning(
            f"couldn't find proxy {handler.proxy_kind}",
            available=list(proxy_dic.keys()),
        )
        p_data = proxy_dic[DEFAULT_PROXY.name]

    caps = [p_data.cap]
    if handler.needs_browser:
        if not handler.headless:
            caps.append(Caps.display)
        if handler.eager:
            caps.append(Caps.eager_browser)
        else:
            caps.append(Caps.normal_browser)
    return caps


class ConnectionSession(ActorBase):
    def __init__(
        self,
        is_browser=False,
        headless=True,
        eager=False,
        proxy_kls=DEFAULT_PROXY,
    ):
        self.is_browser = is_browser
        self.eager = eager
        self._proxy_kls = proxy_kls()
        self.proxy_host = self._proxy_kls.get_new_host()

        self._insess = (
            BrowserSession(headless, self.eager)
            if self.is_browser
            else RequestSession()
        )
        self._initiated_handlers = set()
        self._broken_handlers = set()
        self._num_queries = 0
        self._insess.start(self._proxy_kls, self.proxy_host)

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
        self._insess.stop()

    def get_parsed_response(self, handler: UrlHandler, url, params=None):
        """
        returns json serializable or dies trying
        """
        resp = self._insess.get_response_content(handler, url, params)
        if isinstance(resp, (dict, list)):
            return resp
        if handler.parses_raw:
            return handler.parse_raw(resp)
        elif handler.parses_json:
            try:
                ser = json.loads(resp)
            except JSONDecodeError:
                ser = json.loads(BeautifulSoup(resp, "html5lib").text)
            return handler.parse_json(ser)
        else:
            return handler.parse_soup(BeautifulSoup(resp, "html5lib"))

    def _restart(self, new_proxy=True):
        self._insess.stop()
        if new_proxy:
            self.proxy_host = self._proxy_kls.get_new_host()
        self._initiated_handlers = set()
        self._broken_handlers = set()
        self._insess.start(self._proxy_kls, self.proxy_host)
        self._num_queries = 0

    def _get_uh_result(
        self,
        task: HandlingTask,
    ) -> UrlHandlerResult:
        try:
            out = self.get_parsed_response(task.handler, task.url)
            status = Statuses.PROCESSED
        except Exception as e:
            out = _parse_exception(e)
            status = EXCEPTION_STATUSES.get(type(e), DEFAULT_EXCEPTION_STATUS)
            logger.warning(
                "Error while getting parsed response",
                handler=task.handler_name,
                url=task.url,
                proxy_fstring=self.proxy_host,
                **out,
            )

        outfile = task.object_store.dump_json(out) if out is not None else out

        return UrlHandlerResult(
            handler_name=task.handler_name,
            url=task.url,
            timestamp=int(time.time()),
            output_file=outfile,
            status=status,
            expiration_seconds=task.handler.expiration_seconds,
            registered_links=task.handler.pop_registered_links(),
        )

    def _initiate_handler(self, handler: UrlHandler):
        for _ in range(handler.initiation_retries):
            try:
                self._insess.initiate_handler(handler)
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
        self.browser: Optional[Chrome] = None

    def start(self, proxy_kls: ProxyBase, proxy_host: str):
        chrome_options = proxy_kls.chrome_optins_from_host(proxy_host)
        caps = DesiredCapabilities().CHROME
        if sys.platform == "linux":
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--no-sandbox")  # linux only
        if self._headless:
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--headless")
        if self._eager:
            caps["pageLoadStrategy"] = "eager"
        logger.info(f"launching browser: {chrome_options.arguments}")
        self.browser = Chrome(options=chrome_options, desired_capabilities=caps)

    def stop(self):
        try:
            self.browser.close()
        except WebDriverException:
            logger.warning("could not stop browser")

    def initiate_handler(self, handler: "UrlHandler"):
        handler.start_browser_session(self.browser)

    def get_response_content(
        self,
        handler: UrlHandler,
        url: str,
        params: Optional[dict] = None,
    ):
        if params:
            url = add_url_params(url, params)
        self.browser.get(url)

        out = handler.handle_browser(self.browser)
        if out is not None:
            return out

        return self.browser.page_source


class RequestSession:
    def __init__(self):
        self.rsession: Optional[requests.Session] = None

    def start(self, proxy_kls: ProxyBase, proxy_host: str):
        self.rsession = requests.Session()
        self.rsession.headers.update(HEADERS)
        self.rsession.proxies.update(proxy_kls.rdict_from_host(proxy_host))

    def stop(self):
        pass

    def initiate_handler(self, handler: "UrlHandler"):
        handler.start_rsession(self.rsession)

    def get_response_content(
        self,
        handler: UrlHandler,
        url: str,
        params: Optional[dict] = None,
    ):
        for attempt in range(handler.max_retries):
            try:
                resp = self.rsession.get(url, params=params)
                if resp.ok:
                    return resp.content
                req_result = resp.status_code
            except requests.exceptions.ConnectionError as e:
                req_result = f"connection error - {e}"
            logger.warning(
                "unsuccessful request",
                req_result=req_result,
                url=url,
                attempt=attempt,
            )
            if _is_session_broken(req_result):
                raise BrokenSessionError(f"{req_result} - :(")
            time.sleep(handler.get_retry_sleep_time())
        raise RequestError(f"request resulted in error with status {req_result}")


cap_to_kwarg = {
    Caps.display: dict(headless=False),
    Caps.normal_browser: dict(is_browser=True),
    Caps.eager_browser: dict(is_browser=True, eager=True),
}


def get_actor_dict(proxy_data: Iterable[ProxyData]):

    browsets = [[Caps.eager_browser], [Caps.normal_browser]]
    base_capsets = [[Caps.simple]] + browsets + [[Caps.display, *bs] for bs in browsets]
    out = {}
    for pd, capset in product(proxy_data, base_capsets):
        full_kwargs = dict(proxy_kls=pd.kls)
        for cap in capset:
            full_kwargs.update(cap_to_kwarg.get(cap, {}))
        actor = partial(ConnectionSession, **full_kwargs)
        actor.__name__ = "Conn"  # TODO hack :(
        out[CapabilitySet([pd.cap, *capset])] = actor

    return out


def _is_session_broken(req_res):  # TODO: this might be handle specific
    if req_res == 404:
        return False
    if isinstance(req_res, int):
        return True
    if req_res.startswith("connection error - "):
        return True


def _parse_exception(e):
    return {
        "e_type": type(e).__name__,
        "e": str(e),
        "tb": [tb.strip().split("\n") for tb in traceback.format_tb(e.__traceback__)],
    }
