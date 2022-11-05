import pytest

from aswan import Project
from aswan.tests.godel_src.handlers import AuthedProxy
from aswan.url_handler import BrowserHandler, RequestHandler


def test_run_preps(test_project2: Project):
    @test_project2.register_handler
    class AH(RequestHandler):
        test_urls = ["test-1"]

    @test_project2.register_handler
    class BH(BrowserHandler):
        pass


def test_wrong_setup(test_project2: Project):
    @test_project2.register_handler
    class BH(BrowserHandler):
        proxy_cls = AuthedProxy
        headless: bool = True

    with pytest.raises(RuntimeError):
        test_project2._create_scheduler()
