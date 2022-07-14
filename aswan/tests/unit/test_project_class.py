from aswan import Project
from aswan.config_class import AswanConfig
from aswan.constants import Statuses
from aswan.models import CollectionEvent
from aswan.url_handler import RequestHandler

from .test_metadata_handling import _getcev


def test_coll_event_handling(test_project: Project):

    _meta = test_project._meta

    class At(RequestHandler):
        pass

    class Bt(RequestHandler):
        pass

    coll_events = [
        [10, "At", "x", "of1", Statuses.PROCESSED],
        [12, "At", "x", "of2", Statuses.PROCESSED],
        [12, "At", "y", "of3", Statuses.CONNECTION_ERROR],
        [10, "Bt", "x", "of4", Statuses.PROCESSED],
        [13, "Bt", "z", "of5", Statuses.PROCESSED],
        [14, "Bt", "z", "of6", Statuses.PARSING_ERROR],
    ]

    with _meta._get_session() as session:
        for _ts, _h, _url, _of, _st in coll_events:
            session.add(
                CollectionEvent(
                    timestamp=_ts,
                    handler=_h,
                    url=_url,
                    output_file=_of,
                    status=_st,
                )
            )
        session.commit()

    cevs1 = _meta.get_handler_events(At, only_latest=True, only_successful=True)

    assert len(cevs1) == 1
    assert cevs1[0].output_file == "of2"

    cevs2 = _meta.get_handler_events(At, only_latest=False, only_successful=True)
    assert len(cevs2) == 2
    assert set([cev.output_file for cev in cevs2]) == {"of2", "of1"}

    cevs3 = _meta.get_handler_events(Bt, only_latest=True, only_successful=False)
    assert len(cevs3) == 2
    assert set([cev.output_file for cev in cevs3]) == {"of4", "of6"}

    cevs4 = _meta.get_handler_events(Bt, only_latest=False, only_successful=False)
    assert len(cevs4) == 3
    assert set([cev.output_file for cev in cevs4]) == {"of4", "of5", "of6"}


def test_run_preps(test_project: Project):
    @test_project.register_handler
    class AH(RequestHandler):
        test_urls = ["test-1"]

    @test_project.register_handler
    class BH(RequestHandler):
        pass

    test_project.set_env(AswanConfig.test_name)
    test_project._prepare_run(with_monitor_process=False)
    test_project._finalize_run(with_monitor_process=False)


def test_t2_integration(test_project: Project):
    _meta = test_project._meta
    with _meta._get_session() as session:
        session.add(_getcev())
        session.commit()

    with _meta.get_non_integrated("ABC") as pcevs:
        assert len(pcevs) == 1

    with _meta.get_non_integrated("ABC") as pcevs:
        assert len(pcevs) == 0

    with _meta.get_non_integrated("XYZ") as pcevs:
        assert len(pcevs) == 1
