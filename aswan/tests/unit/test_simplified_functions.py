from bs4 import BeautifulSoup
from requests.utils import default_headers

from aswan import RequestSoupHandler
from aswan.constants import DEPOT_ROOT_ENV_VAR
from aswan.depot import AswanDepot
from aswan.simplified_functions import get_json, get_soup, run_simple_project
from aswan.tests.godel_src.app import test_app_default_address


class _LinkHandler(RequestSoupHandler):
    url_root = test_app_default_address

    def parse(self, soup: BeautifulSoup):
        return {"first_link": soup.find("a").text}


def test_get_soup(godel_test_app):
    h = default_headers()
    soup = get_soup(f"{test_app_default_address}/test_page/godel_wiki.html", headers=h)
    assert isinstance(soup, BeautifulSoup)
    assert soup.find("a").text == "axiomatic"


def test_get_json(godel_test_app):
    d = get_json(f"{test_app_default_address}/test_page/test_json.json")
    assert isinstance(d, dict)
    assert d["A"] == 10


def test_run_simple_project(godel_test_app, tmp_path, monkeypatch):
    monkeypatch.setenv(DEPOT_ROOT_ENV_VAR, str(tmp_path))
    run_simple_project({_LinkHandler: ["/test_page/godel_wiki.html"]}, "simple-proj")
    depot = AswanDepot("simple-proj", tmp_path)
    contents = [ev.content for ev in depot.get_handler_events(_LinkHandler)]
    assert {"first_link": "axiomatic"} in contents
