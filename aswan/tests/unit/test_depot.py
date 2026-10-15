import os
from functools import partial
from pathlib import Path

from atqo import parallel_map

import aswan
from aswan.constants import Statuses
from aswan.depot.base import (
    CONTEXT_JSON,
    Run,
    STATUS_DB_ZIP,
    STATUS_RUNS_JSON,
    get_sorted_coll_events,
)
from aswan.models import CollEvent

from ..helpers import get_cev, seed_run, statuses


def test_coll_event_handling(test_depot: aswan.AswanDepot):
    class At(aswan.RequestHandler):
        pass

    class Bt(aswan.RequestHandler):
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

    test_depot.current.integrate_events(coll_events)
    _ghe = partial(test_depot.get_handler_events, from_current=True)

    cevs1 = [*_ghe(At, only_latest=True, only_successful=True)]
    assert len(cevs1) == 1
    assert cevs1[0].cev.extend().output_file == "of2"

    cevs2 = [*_ghe(At, only_latest=False, only_successful=True)]
    assert len(cevs2) == 2
    assert set([cev.cev.extend().output_file for cev in cevs2]) == {"of2", "of1"}

    cevs3 = [*_ghe(Bt, only_latest=True, only_successful=False)]
    assert len(cevs3) == 2
    assert set([cev.cev.extend().output_file for cev in cevs3]) == {"of4", "of6"}

    cevs4 = [*_ghe(Bt, only_latest=False, only_successful=False)]
    assert len(cevs4) == 3
    assert set([cev.cev.extend().output_file for cev in cevs4]) == {"of4", "of5", "of6"}


def test_getting_latest_run(test_depot: aswan.AswanDepot):
    test_depot.current.integrate_events([get_cev(output_file="of-x")])
    first_status = test_depot.save_current()  # returns Status object
    test_depot.current.purge()
    test_depot.init_w_complete()
    test_depot.current.integrate_events([get_cev(output_file="of-y")])
    test_depot.save_current()
    assert next(test_depot.get_handler_events()).cev.output_file == "of-y"
    assert len(list(test_depot.get_handler_events(post_status=first_status))) == 1


def test_parallel_proc(test_depot: aswan.AswanDepot):
    test_depot.current.integrate_events([get_cev(output_file="of-x")])
    test_depot.save_current()
    lc = list(parallel_map(str, test_depot.get_handler_events(from_current=True)))
    assert lc
    lo = list(parallel_map(str, test_depot.get_handler_events()))
    assert lo == lc


def test_depobj_init(tmp_path):
    wd = Path.cwd()
    os.chdir(tmp_path)
    try:
        Run()
    finally:
        os.chdir(wd)


def test_depot_cleanup(test_depot: aswan.AswanDepot):
    seed_run(test_depot, output_file="of-x")
    seed_run(test_depot, output_file="of-y")
    run_dirs = list(test_depot.runs_path.iterdir())
    assert len(run_dirs) == 2
    for run_dir in run_dirs:
        assert (run_dir / CONTEXT_JSON).exists()


def test_depot_restatus(test_depot: aswan.AswanDepot):
    test_depot.current.integrate_events([get_cev(output_file="of-x")])
    test_depot.save_current()
    # Remove the status files to force recomputation
    (test_depot.root / STATUS_DB_ZIP).unlink(missing_ok=True)
    (test_depot.root / STATUS_RUNS_JSON).unlink(missing_ok=True)
    new_comp = test_depot.get_complete_status()
    assert len(new_comp.integrated_runs) == 1
    still_comp = test_depot.get_complete_status()
    assert still_comp.name == new_comp.name


def test_restatus_without_status_db_replays_every_run(test_depot: aswan.AswanDepot):
    seed_run(test_depot, url="u1")
    seed_run(test_depot, url="u2")
    (test_depot.root / STATUS_DB_ZIP).unlink()
    assert set(statuses(test_depot)) == {("A", "u1"), ("A", "u2")}


def test_latest_event_is_the_retry_of_a_same_second_session_break():
    broken = get_cev(status=Statuses.SESSION_BROKEN, timestamp=5)
    failed = get_cev(status=Statuses.CONNECTION_ERROR, timestamp=5)
    for events in ([broken, failed], [failed, broken]):
        assert next(get_sorted_coll_events(events)).status == failed.status
