import json
import random
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, List, Optional, Type, Union
from urllib.parse import urljoin

import requests
from structlog import get_logger

if TYPE_CHECKING:
    from bs4 import BeautifulSoup  # pragma: no cover
    from selenium.webdriver import Chrome  # pragma: no cover

logger = get_logger()


class UrlHandlerBase:
    starter_urls: Iterable[str] = []
    test_urls: Iterable[str] = []
    url_root: Optional[str] = None
    default_expiration: int = -1
    proxy_kind: Optional[str] = None

    max_retries: int = 2
    initiation_retries: int = 2
    wait_on_initiation_fail: int = 20
    restart_session_after: int = 50
    # TODO: make ignore/transfer cookies possible

    def __init__(self):
        self._registered_links = []
        self._url: Optional[str] = None
        self.expiration_seconds = self.default_expiration

    def set_url(self, url):
        self._url = url

    def pre_parse(self, blob):
        return blob

    def set_expiration(self, exp_secs):
        self.expiration_seconds = exp_secs

    def reset_expiration(self):
        self.expiration_seconds = self.default_expiration

    def register_links_to_handler(
        self,
        links: Iterable[str],
        handler_cls: Optional[Type["ANY_HANDLER_T"]] = None,
        expiration_time: Optional[int] = None,
    ):
        if handler_cls is None:
            handler_cls = type(self)
        if expiration_time is None:
            if handler_cls == type(self):
                expiration_time = self.expiration_seconds
            else:
                expiration_time = handler_cls.default_expiration
        self._registered_links += [
            RegisteredLink(
                handler_cls=handler_cls,
                url=self.extend_link(link),
                expiration=expiration_time,
            )
            for link in links
        ]

    def pop_registered_links(self) -> List["RegisteredLink"]:
        out = self._registered_links
        self._registered_links = []
        return out

    @classmethod
    def extend_link(cls, link: str) -> str:
        return urljoin(cls.url_root, link)

    @property
    def name(self):
        return type(self).__name__

    @staticmethod
    def get_restart_sleep_time():
        return random.uniform(0.2, 1.2)

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
        """handle session every time before getting url"""

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
        """if returns something, that is forwarded to parse"""

    def start_session(self, browser: "Chrome"):
        """start session if browser is needed"""

    def is_session_broken(self, result: Exception):
        """exception when getting source

        if error occurs during handle browser, it comes here
        """
        return True


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


@dataclass
class RegisteredLink:
    handler_cls: Type[ANY_HANDLER_T]
    url: str
    expiration: int
