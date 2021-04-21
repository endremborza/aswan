from bs4 import BeautifulSoup

from aswan.simplified_functions import get_json, get_soup
from aswan.tests.godel_src.app import test_app_default_address


def test_get_soup(godel_test_app):
    soup = get_soup(f"{test_app_default_address}/test_page/godel_wiki.html")
    assert isinstance(soup, BeautifulSoup)
    assert soup.find("a").text == "axiomatic"


def test_get_json(godel_test_app):
    d = get_json(f"{test_app_default_address}/test_page/test_json.json")
    assert isinstance(d, dict)
    assert d["A"] == 10
