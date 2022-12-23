import pytest

from aswan import Project
from aswan.models import RegEvent
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


class CLH(RequestHandler):
    def load_cache(self, url):  # pragma: no cover
        return "thing"


def test_overflow(test_project2: Project):
    test_project2.debug = True
    test_project2.max_cpu_use = 2
    test_project2.batch_size = 5
    test_project2.min_queue_size = 3
    test_project2.run(urls_to_register={CLH: list(map(str, range(30)))})


def test_commit_refuse(test_project2: Project):
    depot = test_project2.depot.setup(init=True)
    depot.current.integrate_events([RegEvent("link-1", "H")])
    depot.current.next_batch(2)
    with pytest.raises(ValueError):
        test_project2.commit_current_run()
