import json
import random
from collections.abc import Iterable
from typing import TYPE_CHECKING
from urllib.parse import urljoin

import requests

from .constants import RELAY_CONTENT_K, RELAY_PORT, RELAY_REG_K
from .models import RegEvent
from .resources import CPU_BROWSER, CPU_SIMPLE, DISPLAY, MCPU
from .security import DEFAULT_PROXY, ProxyBase
from .utils import add_url_params

if TYPE_CHECKING:
    from bs4 import BeautifulSoup  # pragma: no cover
    from selenium.webdriver import Chrome  # pragma: no cover


class UrlHandlerBase:
    test_urls: Iterable[str] = []
    url_root: str | None = None
    proxy_cls: type[ProxyBase] = DEFAULT_PROXY

    max_in_parallel: int | None = None
    max_retries: int = 2
    initiation_retries: int = 2
    wait_on_initiation_fail: int = 20
    restart_session_after: int = 50

    def __init__(self):
        self.proxy = self.proxy_cls()
        self._registered_links = []
        self._url: str | None = None

    def set_url(self, url):
        self._url = url

    def pre_parse(self, blob):
        return blob

    def load_cache(self, url):
        return None

    def register_links_to_handler(
        self,
        links: Iterable[str],
        handler_cls: "type[ANY_HANDLER_T] | None" = None,
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

    def pop_registered_links(self) -> list["RegEvent"]:
        out = self._registered_links
        self._registered_links = []
        return out

    def get_requirements(self) -> dict[str, int]:
        reqs: dict[str, int] = dict(self.proxy.requirements)
        if isinstance(self, BrowserHandler):
            reqs[MCPU] = CPU_BROWSER
            if not self.headless:  # pragma: no cover
                reqs[DISPLAY] = 1
                reqs[MCPU] = CPU_SIMPLE
        else:
            reqs[MCPU] = CPU_SIMPLE
        if self.max_in_parallel is not None:
            reqs[self.name] = 1
        return reqs

    @classmethod
    def extend_link(cls, link: str) -> str:
        try:
            from bs4 import Tag

            if isinstance(link, Tag):
                link = link["href"]
        except ImportError:
            pass
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

    def is_session_broken(self, result: "int | Exception"):
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


class RelayHandler(UrlHandlerBase):
    """Delegates collection to an external worker over an HTTP port.

    Override ``make_command`` to trigger the worker for a url, and
    ``make_followup`` to issue a command derived from the response. A
    pull-based worker can instead poll ``GET /url`` for the pending url. The
    worker POSTs ``{url, content, register?}`` back to the port.

    ``relay_timeout`` bounds one wait for the worker's POST (polled every
    ``wait_time`` seconds); ``max_retries`` counts whole attempts, each of
    which re-issues ``make_command``.
    """

    max_in_parallel = 1
    listen_port = RELAY_PORT
    wait_time = 1
    relay_timeout = 5
    max_retries = 2
    restart_session_after = float("inf")

    def make_command(self, url: str) -> "list[str] | None":
        """command issued to trigger external collection of ``url``"""
        return None

    def make_followup(self, response: bytes) -> "list[str] | None":
        """command issued after the response arrives, may derive from it"""
        return None

    def parse(self, response: bytes):
        resp_dic: dict = json.loads(response)
        self.register_links_to_handler(resp_dic.get(RELAY_REG_K, []))
        return resp_dic[RELAY_CONTENT_K]

    def handle_driver(self, _):
        pass

    def start_session(self, _):
        pass

    def is_session_broken(self, result: "int | Exception"):
        return False


ANY_HANDLER_T = RequestHandler | BrowserHandler | RelayHandler


class _SoupMixin:
    def pre_parse(self, blob: bytes):
        from bs4 import BeautifulSoup

        return BeautifulSoup(blob, "html.parser")

    def parse(self, soup: "BeautifulSoup"):
        """parse bs4 soup"""
        return soup


class _JsonMixin:
    def pre_parse(self, blob: bytes):
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
