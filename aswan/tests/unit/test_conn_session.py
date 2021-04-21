from aswan import UrlHandler
from aswan.connection_session import ConnectionSession, HandlingTask
from aswan.constants import Statuses
from aswan.object_store import get_object_store
from aswan.tests.godel_src.app import test_app_default_address


def test_godel_defaults(tmp_path, godel_test_app):

    ostr = get_object_store(str(tmp_path))
    cm = ConnectionSession([])

    url = f"{test_app_default_address}/test_page/Axiom.html"
    task = HandlingTask(UrlHandler(), url, ostr)

    uh_res = cm.consume(task)
    cm.stop()

    assert uh_res.url == url
    assert uh_res.handler_name == "UrlHandler"
    assert uh_res.status == Statuses.PROCESSED
