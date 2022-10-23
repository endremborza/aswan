from aswan import Project
from aswan.constants import Statuses
from aswan.depot import Status
from aswan.models import CollEvent
from aswan.url_handler import RequestHandler


def test_coll_event_handling(test_project2: Project):

    test_project2.depot.setup()
    test_project2.depot.set_as_current(Status())
    depot = test_project2.depot

    class At(RequestHandler):
        pass

    class Bt(RequestHandler):
        pass

    coll_events = map(
        lambda args: CollEvent(*args),
        [
            ["x", "At", Statuses.PROCESSED, 10, "of1"],
            ["x", "At", Statuses.PROCESSED, 12, "of2"],
            ["y", "At", Statuses.CONNECTION_ERROR, 12, "of3"],
            ["x", "Bt", Statuses.PROCESSED, 10, "of4"],
            ["z", "Bt", Statuses.PROCESSED, 13, "of5"],
            ["z", "Bt", Statuses.PARSING_ERROR, 14, "of6"],
        ],
    )

    depot.current.integrate_events(coll_events)

    cevs1 = [*test_project2.handler_events(At, only_latest=True, only_successful=True)]

    assert len(cevs1) == 1
    assert cevs1[0].output_file == "of2"

    cevs2 = [*test_project2.handler_events(At, only_latest=False, only_successful=True)]
    assert len(cevs2) == 2
    assert set([cev.output_file for cev in cevs2]) == {"of2", "of1"}

    cevs3 = [*test_project2.handler_events(Bt, only_latest=True, only_successful=False)]
    assert len(cevs3) == 2
    assert set([cev.output_file for cev in cevs3]) == {"of4", "of6"}

    cevs4 = [
        *test_project2.handler_events(Bt, only_latest=False, only_successful=False)
    ]
    assert len(cevs4) == 3
    assert set([cev.output_file for cev in cevs4]) == {"of4", "of5", "of6"}


def test_run_preps(test_project2: Project):
    @test_project2.register_handler
    class AH(RequestHandler):
        test_urls = ["test-1"]

    @test_project2.register_handler
    class BH(RequestHandler):
        pass
