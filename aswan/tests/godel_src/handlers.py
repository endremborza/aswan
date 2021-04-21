from bs4 import BeautifulSoup
from selenium.webdriver import Chrome

import aswan
from aswan.tests.godel_src.app import test_app_default_address


class GetMain(aswan.UrlHandler):
    url_root = test_app_default_address
    test_urls = ["/test_page/Alonzo_Church.html"]

    def parse_soup(self, soup: BeautifulSoup):
        return {"main": soup.find("b").text.strip()}


class Clicker(aswan.UrlHandler):
    url_root = test_app_default_address
    needs_browser = True
    headless = True
    test_urls = ["/test_page/jstest_bad.html"]

    def handle_browser(self, browser: Chrome):
        browser.find_element_by_id("funbut").click()
        out_time = int(browser.find_element_by_id("field4").text)
        if int(out_time) > 2000:
            self.set_expiration(0)

        return {
            "field4": out_time,
            "field2": browser.find_element_by_id("field2").text,
        }


class LinkRoot(aswan.UrlHandler):
    url_root = test_app_default_address
    starter_urls = ["/test_page/godel_wiki.html"]
    test_urls = ["/test_page/godel_wiki.html"]

    def parse_soup(self, soup: BeautifulSoup):
        for a in soup.find_all("a"):
            link = a["href"]
            if a.get("id", "") == "interactive":
                self.register_link_to_handler(link, Clicker)
            else:
                self.register_links_to_handler([link], GetMain)


class JS(aswan.UrlJsonHandler):
    url_root = test_app_default_address
    starter_urls = ["/test_page/test_json.json"]
    test_urls = ["/test_page/test_json.json"]

    def parse_json(self, obj):
        return {"url": self._url, **obj}


# test json
# test expiry with clickable thingy
# conditional expiry setting
# integrators
