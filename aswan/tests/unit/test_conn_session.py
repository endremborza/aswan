from bs4 import BeautifulSoup

import aswan.tests.godel_src.handlers as ghandlers
from aswan import (
    AswanDepot,
    BrokenSessionError,
    BrowserSoupHandler,
    RequestJsonHandler,
    RequestSoupHandler,
    Statuses,
)
from aswan.connection_session import ConnectionSession, HandlingTask
from aswan.tests.godel_src.app import test_app_default_address

_URL = f"{test_app_default_address}/test_page/Axiom.html"
_404_URL = f"{test_app_default_address}/test_page/Nonexistent"


class _Setup:
    def __init__(self, tmp_path, Handler, url=_URL, browser=False) -> None:

        self.depot = AswanDepot("depot", tmp_path)
        self.cm = ConnectionSession(is_browser=browser, depot_path=self.depot.root)
        self.task = HandlingTask(Handler(), url)
        self.ostr = self.cm.store

    def run(self):
        return self.cm.consume(self.task)

    def get_res(self, succ=False):
        return next(
            self.depot.get_handler_events(from_current=True, only_successful=succ)
        )


def test_godel_defaults(tmp_path, godel_test_app):
    class H(RequestSoupHandler):
        def parse(self, soup: "BeautifulSoup"):
            return {"url": self._url, "links": soup.find_all("a")}

    setup = _Setup(tmp_path, H)

    setup.run()
    setup.cm.stop()

    uh_res = setup.get_res()

    assert uh_res.url == _URL
    assert uh_res.handler_name == "H"
    assert uh_res.status == Statuses.PROCESSED


def test_session_breaking(tmp_path, godel_test_app):
    class BH(BrowserSoupHandler):
        # TODO: even though I make browser soup handler
        # conn session needs to be set to browser...
        _run = 0

        def parse(self, soup: "BeautifulSoup"):
            if self._run < 1:
                self._run = 2
                raise BrokenSessionError("")
            return soup.find_all("a")

    setup = _Setup(tmp_path, BH, browser=True)
    setup.run()
    uh_res = setup.get_res()
    assert uh_res.status == Statuses.SESSION_BROKEN

    setup.run()
    uh_res2 = setup.get_res(succ=True)
    assert uh_res2.status == Statuses.PROCESSED
    setup.cm.stop()


def test_caching(tmp_path, godel_test_app):
    class H(RequestSoupHandler):
        def load_cache(self, url):
            if url == _URL:
                return {"x": 742}

    setup = _Setup(tmp_path, H)
    setup.run()
    uhr = setup.get_res()
    assert uhr.content == {"x": 742}


def test_404_err(tmp_path, godel_test_app, test_proxy):

    s2 = _Setup(tmp_path, ghandlers.GetMain, _404_URL)
    s2.run()
    uhr = s2.get_res()
    assert uhr.status == Statuses.SESSION_BROKEN


def test_url_params(godel_test_app):

    cm = ConnectionSession()

    out = cm.get_parsed_response(
        f"{test_app_default_address}/test_param",
        RequestJsonHandler(),
        params={"param": r'{"s":10}'},
    )
    assert out == {"s": 10}
