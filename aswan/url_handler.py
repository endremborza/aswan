import random
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, List, Optional, Type, Union

from structlog import get_logger

from .resources import (
    BrowserResource,
    EagerBrowserResource,
    HeadlessBrowserResource,
    ProxyResource,
    UrlBaseConnection,
)
from .scheduler.resource import Resource
from .security import DEFAULT_PROXY

if TYPE_CHECKING:
    import requests  # pragma: no cover
    from bs4 import BeautifulSoup  # pragma: no cover
    from selenium.webdriver import Chrome  # pragma: no cover

logger = get_logger()


class _HandlerBase:
    starter_urls: Iterable[str] = []
    test_urls: Iterable[str] = []
    url_root: Optional[str] = None
    default_expiration: int = -1
    proxy_kind: Optional[str] = None

    max_retries: int = 2
    initiation_retries: int = 2
    wait_on_initiation_fail: int = 20

    restart_session_after: int = 50
    # TODO: make ignore cookies possible

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


class UrlHandler(_HandlerBase):
    needs_browser: bool = False
    headless: bool = False
    eager: bool = False
    parses_raw: bool = False
    parses_json: bool = False

    def __init__(self):
        self._registered_links = []
        # self.register_links_to_handler(self.starter_urls)
        self._url: Optional[str] = None
        self.expiration_seconds = self.default_expiration

    def parse_soup(self, soup: "BeautifulSoup"):
        """ parse bs4 soup"""

    def parse_json(self, obj: Union[list, dict]):
        """ if parses_json is true"""

    def parse_raw(self, s: str):
        """ if parses_raw is true"""

    def handle_browser(self, driver: "Chrome"):
        """ if returns something, parse soup is not called"""

    def start_rsession(self, rsession: "requests.Session"):
        """ starts session if no browser needed"""

    def start_browser_session(self, browser: "Chrome"):
        """ start session if browser is needed"""

    def register_links_to_handler(
        self,
        links: Iterable[str],
        handler_cls: Optional[Type["UrlHandler"]] = None,
    ):
        self._register_links_to_handler(links, handler_cls)

    def register_link_to_handler(
        self,
        link: str,
        handler_cls: Optional[Type["UrlHandler"]] = None,
    ):
        self._register_links_to_handler([link], handler_cls)

    def pop_registered_links(self) -> List["RegisteredLink"]:
        out = self._registered_links
        self._registered_links = []
        return out

    def get_resource_needs(self, proxy_dic) -> List[Resource]:
        # TODO: maybe needs to be better
        resources = [UrlBaseConnection(self.url_root)]
        if self.needs_browser:
            if self.headless:
                resources.append(HeadlessBrowserResource())
            else:
                resources.append(BrowserResource())
            if self.eager:
                resources.append(EagerBrowserResource())
        if self.proxy_kind is not None:
            try:
                proxy_kls = proxy_dic[self.proxy_kind]
            except KeyError:
                logger.info(
                    f"couldn't find proxy {self.proxy_kind}",
                    available=list(proxy_dic.keys()),
                )
                proxy_kls = DEFAULT_PROXY
            resources.append(ProxyResource(proxy_kls))
        return resources

    def set_url(self, url):
        self._url = url

    def set_expiration(self, exp_secs):
        self.expiration_seconds = exp_secs

    def reset_expiration(self):
        self.expiration_seconds = self.default_expiration

    @classmethod
    def extend_link(cls, link) -> str:
        # TODO: this is shit
        if cls.url_root is None:
            return link
        if link.startswith(cls.url_root):
            return link
        if link.startswith("/"):
            return cls.url_root + link
        return link

    def _register_links_to_handler(
        self,
        links: Iterable[str],
        handler_cls: Optional[Type["UrlHandler"]],
    ):
        if handler_cls is None:
            handler_cls = type(self)
        self._registered_links += [
            RegisteredLink(
                handler_name=handler_cls.__name__,
                url=self.extend_link(link),
            )
            for link in links
        ]


class UrlJsonHandler(UrlHandler):
    parses_json = True


@dataclass
class RegisteredLink:
    handler_name: str
    url: str
