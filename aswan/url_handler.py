import json
import random
from typing import TYPE_CHECKING, Iterable, List, Optional, Type, Union
from urllib.parse import urljoin

import requests
from atqo import Capability, CapabilitySet
from bs4 import BeautifulSoup, Tag
from structlog import get_logger

from .models import RegEvent
from .resources import Caps
from .security import DEFAULT_PROXY, ProxyBase
from .utils import add_url_params

if TYPE_CHECKING:
    from selenium.webdriver import Chrome  # pragma: no cover

logger = get_logger()


class UrlHandlerBase:
    test_urls: Iterable[str] = []
    url_root: Optional[str] = None
    proxy_cls: Type[ProxyBase] = DEFAULT_PROXY

    max_in_parallel: Optional[int] = None
    max_retries: int = 2
    initiation_retries: int = 2
    wait_on_initiation_fail: int = 20
    restart_session_after: int = 50
    process_indefinitely: bool = False

    def __init__(self):
        self.proxy = self.proxy_cls()
        self._registered_links = []
        self._url: Optional[str] = None

    def set_url(self, url):
        self._url = url

    def pre_parse(self, blob):
        return blob

    def load_cache(self, url):
        return None

    def register_links_to_handler(
        self,
        links: Iterable[str],
        handler_cls: Optional[Type["ANY_HANDLER_T"]] = None,
        overwrite: bool = False,
    ):
        if handler_cls is None:
            handler_cls = type(self)
        self._registered_links += [
            RegEvent(
                url=self.extend_link(link),
                handler=handler_cls.__name__,
                overwrite=overwrite,
            )
            for link in links
        ]

    def register_url_with_params(self, params: dict):
        parsed_params = {k: v for k, v in params.items() if v}
        if not parsed_params:
            return
        next_url = add_url_params(self._url, parsed_params)
        self.register_links_to_handler([next_url])

    def pop_registered_links(self) -> List["RegEvent"]:
        out = self._registered_links
        self._registered_links = []
        return out

    def get_caps(self):
        caps = [*self.proxy.caps]
        if isinstance(self, BrowserHandler):
            if not self.headless:  # pragma: no cover
                caps.append(Caps.display)
            if self.eager:  # pragma: no cover
                caps.append(Caps.eager_browser)  # how to test?
            else:
                caps.append(Caps.normal_browser)
        else:
            caps.append(Caps.simple)
        if self.max_in_parallel is not None:
            caps.append(Capability({self.name: 1}, name=f"{self.name}-cap"))
        return CapabilitySet(caps)

    @classmethod
    def extend_link(cls, link: str) -> str:
        if isinstance(link, Tag):
            link = link["href"]
        return urljoin(cls.url_root, link)

    @property
    def name(self):
        return type(self).__name__

    @staticmethod
    def get_retry_sleep_time():
        return random.uniform(0.1, 0.6)

    @staticmethod
    def get_sleep_time():
        return 0


class RequestHandler(UrlHandlerBase):
    def parse(self, blob):
        return blob

    def handle_driver(self, session: "requests.Session"):
        """runs before get. can set/update cookies/headers"""

    def start_session(self, session: "requests.Session"):
        """starts session if no browser needed"""

    def is_session_broken(self, result: Union[int, Exception]):
        """either response code, or exception"""
        # for determining to restart the session,
        # or proceed with handling connection error
        if isinstance(result, int):
            return result != 404
        return True


class BrowserHandler(UrlHandlerBase):
    headless: bool = False
    eager: bool = False

    def parse(self, source):
        return source

    def handle_driver(self, driver: "Chrome"):
        """runs after get. if returns something, that is forwarded to parse"""

    def start_session(self, browser: "Chrome"):
        """start session if browser is needed"""

    def is_session_broken(self, result: Exception):
        """exception when getting source

        if error occurs during handle browser, it comes here
        """
        return False


ANY_HANDLER_T = Union[RequestHandler, BrowserHandler]


class _SoupMixin:
    def pre_parse(self, blob: bytes):
        from bs4 import BeautifulSoup

        return BeautifulSoup(blob, "html5lib")

    def parse(self, soup: "BeautifulSoup"):
        """parse bs4 soup"""
        return soup


class _JsonMixin:
    def pre_parse(self, blob: bytes):
        # except JSONDecodeError:
        # json.loads(BeautifulSoup(resp, "html5lib").text)
        return json.loads(blob)

    def parse(self, obj: dict):
        """if parses_json is true"""
        return obj


class RequestSoupHandler(_SoupMixin, RequestHandler):
    pass


class RequestJsonHandler(_JsonMixin, RequestHandler):
    pass


class BrowserSoupHandler(_SoupMixin, BrowserHandler):
    pass


class BrowserJsonHandler(_JsonMixin, BrowserHandler):
    pass
