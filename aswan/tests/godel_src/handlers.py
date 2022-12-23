from typing import Union

from bs4 import BeautifulSoup
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By

import aswan
from aswan.tests.godel_src.app import test_app_default_address
from aswan.utils import browser_wait

from ..proxy_src import proxy_port, proxy_pw, proxy_user


class TestLoad(aswan.RequestHandler):

    test_urls = ["/test_page/Nonexistent"]

    def load_cache(self, _):
        raise ValueError("")

    def is_session_broken(self, result: Union[int, Exception]):
        return super().is_session_broken(result) and False


class SimpleProxy(aswan.ProxyBase):
    expiration_secs = 0
    port_no = proxy_port

    def _load_host_list(self) -> list:
        return ["localhost"]


class AuthedProxy(SimpleProxy):

    max_at_once = 1

    def get_creds(self):
        return aswan.ProxyAuth(proxy_user, proxy_pw)


class GetMain(aswan.RequestSoupHandler):
    proxy_cls = AuthedProxy
    url_root = test_app_default_address
    test_urls = ["/test_page/Alonzo_Church.html", "/test_page/Nonexistent"]
    process_indefinitely = True
    _vtries = 0

    def parse(self, soup: BeautifulSoup):
        self.register_links_to_handler([])
        return {"main": soup.find("b").text.strip()}

    def is_session_broken(self, result: Union[int, Exception]):
        super().is_session_broken(result)
        if isinstance(result, int) and (result == 404) and (self._vtries < 1):
            self._vtries = 2
            return True


class Clicker(aswan.BrowserHandler):
    proxy_cls = SimpleProxy
    url_root = test_app_default_address
    headless = True
    test_urls = ["/test_page/jstest.html", "/test_page/Nonexistent", "/Broken"]

    def handle_driver(self, browser: Chrome):
        browser_wait(browser, wait_for_id="funbut", timeout=1, click=True)
        out_time = int(browser.find_element(By.ID, "field4").text)
        return {
            "field4": out_time,
            "field2": browser.find_element(By.ID, "field2").text,
        }


class LinkRoot(aswan.RequestSoupHandler):
    url_root = test_app_default_address
    test_urls = ["/test_page/godel_wiki.html"]
    _init_failer = True
    wait_on_initiation_fail = 0
    max_in_parallel = 1

    def parse(self, soup: BeautifulSoup):
        for a in soup.find_all("a"):
            if a.get("id", "") == "interactive":
                _h = Clicker
            else:
                _h = GetMain
            self.register_links_to_handler([a], _h)

    def start_session(self, session):
        if self._init_failer:
            self._init_failer = False
            raise ValueError()


class JS(aswan.RequestJsonHandler):
    url_root = test_app_default_address
    test_urls = ["/test_page/test_json.json"]

    def parse(self, obj):
        return {"url": self._url, **obj}
