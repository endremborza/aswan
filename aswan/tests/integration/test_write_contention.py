"""Many urls of one handler run on several workers at once; every result must
reach the depot and the run must not stall on the current-run SQLite."""

from aswan.constants import Statuses
from aswan.tests.godel_src.handlers import JS

_N_URLS = 12


def _current_events(project):
    return list(
        project.depot.get_handler_events(
            JS, only_successful=False, only_latest=False, from_current=True
        )
    )


def test_many_urls_single_handler_completes(godel_test_app, test_project):
    test_project.register_handler(JS)
    urls = [f"/test_page/test_json.json?n={i}" for i in range(_N_URLS)]
    test_project.run(urls_to_overwrite={JS: urls})

    events = _current_events(test_project)
    assert len(events) == _N_URLS
    assert all(e.status == Statuses.PROCESSED for e in events)


def test_many_failing_urls_single_handler_completes(godel_test_app, test_project):
    test_project.register_handler(JS)
    urls = [f"/test_page/nope_{i}" for i in range(_N_URLS)]
    test_project.run(urls_to_overwrite={JS: urls})

    events = _current_events(test_project)
    assert len(events) == _N_URLS
    assert Statuses.PROCESSING not in {e.status for e in events}
