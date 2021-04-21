from aswan.constants import Envs, Statuses
from aswan.models import CollectionEvent
from aswan.project import project_from_dir
from aswan.tests.utils import change_cwd
from aswan.url_handler import UrlHandler


def test_coll_event_handling(tmp_path):

    with change_cwd(tmp_path):
        _project = project_from_dir()

    class At(UrlHandler):
        pass

    class Bt(UrlHandler):
        pass

    coll_events = [
        [10, "At", "x", "of1", Statuses.PROCESSED],
        [12, "At", "x", "of2", Statuses.PROCESSED],
        [12, "At", "y", "of3", Statuses.CONNECTION_ERROR],
        [10, "Bt", "x", "of4", Statuses.PROCESSED],
        [13, "Bt", "z", "of5", Statuses.PROCESSED],
        [14, "Bt", "z", "of6", Statuses.PARSING_ERROR],
    ]

    with _project._get_session() as session:
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

    cevs1 = _project._get_handler_events(
        At, only_latest=True, only_successful=True
    )

    assert len(cevs1) == 1
    assert cevs1[0].output_file == "of2"

    cevs2 = _project._get_handler_events(
        At, only_latest=False, only_successful=True
    )
    assert len(cevs2) == 2
    assert set([cev.output_file for cev in cevs2]) == {"of2", "of1"}

    cevs3 = _project._get_handler_events(
        Bt, only_latest=True, only_successful=False
    )
    assert len(cevs3) == 2
    assert set([cev.output_file for cev in cevs3]) == {"of4", "of6"}

    cevs4 = _project._get_handler_events(
        Bt, only_latest=False, only_successful=False
    )
    assert len(cevs4) == 3
    assert set([cev.output_file for cev in cevs4]) == {"of4", "of5", "of6"}


def test_run_preps(tmp_path):

    with change_cwd(tmp_path):
        _project = project_from_dir()

    @_project.register_handler
    class AH(UrlHandler):
        test_urls = ["test-1"]

    @_project.register_handler
    class BH(UrlHandler):
        pass

    _project.set_env(Envs.TEST)

    _project._prepare_run(with_monitor_process=False)

    _project._finalize_run(with_monitor_process=False)
