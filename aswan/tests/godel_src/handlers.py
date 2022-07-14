from bs4 import BeautifulSoup
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By

import aswan
from aswan.tests.godel_src.app import test_app_default_address


class GetMain(aswan.RequestSoupHandler):
    url_root = test_app_default_address
    test_urls = ["/test_page/Alonzo_Church.html", "/Nonexistent"]

    def parse(self, soup: BeautifulSoup):
        return {"main": soup.find("b").text.strip()}


class Clicker(aswan.BrowserHandler):
    url_root = test_app_default_address
    headless = True
    test_urls = ["/test_page/jstest.html", "/Nonexistent", "/Broken"]

    def handle_driver(self, browser: Chrome):
        browser.find_element(By.ID, "funbut").click()
        out_time = int(browser.find_element(By.ID, "field4").text)
        if int(out_time) > 2000:
            self.set_expiration(0)

        return {
            "field4": out_time,
            "field2": browser.find_element(By.ID, "field2").text,
        }


class LinkRoot(aswan.RequestSoupHandler):
    url_root = test_app_default_address
    starter_urls = ["/test_page/godel_wiki.html"]
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
    starter_urls = ["/test_page/test_json.json"]
    test_urls = ["/test_page/test_json.json"]

    def parse(self, obj):
        return {"url": self._url, **obj}


# test json
# test expiry with clickable thingy
# conditional expiry setting
# integrators
