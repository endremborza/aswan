from bs4 import BeautifulSoup
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By

import aswan
from aswan.tests.godel_src.app import test_app_default_address
from aswan.utils import browser_wait

from ..proxy_src import proxy_port, proxy_pw, proxy_user


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
    test_urls = ["/test_page/Alonzo_Church.html", "/Nonexistent"]

    def parse(self, soup: BeautifulSoup):
        return {"main": soup.find("b").text.strip()}


class Clicker(aswan.BrowserHandler):
    proxy_cls = SimpleProxy
    url_root = test_app_default_address
    headless = True
    test_urls = ["/test_page/jstest.html", "/Nonexistent", "/Broken"]

    def handle_driver(self, browser: Chrome):
        browser_wait(browser, wait_for_id="funbut", timeout=1, click=True)
        out_time = int(browser.find_element(By.ID, "field4").text)
        return {
            "field4": out_time,
            "field2": browser.find_element(By.ID, "field2").text,
        }

    def is_session_broken(self, result: Exception):
        return False


class LinkRoot(aswan.RequestSoupHandler):
    url_root = test_app_default_address
    test_urls = ["/test_page/godel_wiki.html"]

    def parse(self, soup: BeautifulSoup):
        for a in soup.find_all("a"):
            link = a["href"]
            if a.get("id", "") == "interactive":
                _h = Clicker
            else:
                _h = GetMain
            self.register_links_to_handler([link], _h)


class JS(aswan.RequestJsonHandler):
    url_root = test_app_default_address
    test_urls = ["/test_page/test_json.json"]

    def parse(self, obj):
        return {"url": self._url, **obj}
