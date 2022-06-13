from bs4 import BeautifulSoup

from aswan import RequestSoupHandler
from aswan.connection_session import ConnectionSession, HandlingTask
from aswan.constants import Statuses
from aswan.object_store import ObjectStore
from aswan.tests.godel_src.app import test_app_default_address


def test_godel_defaults(tmp_path, godel_test_app):

    ostr = ObjectStore(tmp_path)
    cm = ConnectionSession()

    url = f"{test_app_default_address}/test_page/Axiom.html"

    class H(RequestSoupHandler):
        def parse(self, soup: "BeautifulSoup"):
            return {"url": self._url, "links": soup.find_all("a")}

    task = HandlingTask(H(), url, ostr)

    uh_res = cm.consume(task)
    cm.stop()

    assert uh_res.url == url
    assert uh_res.handler_name == "H"
    assert uh_res.status == Statuses.PROCESSED
