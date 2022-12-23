import os
from functools import partial
from pathlib import Path

from atqo import parallel_map

import aswan
from aswan.constants import Statuses
from aswan.depot.base import Run
from aswan.models import CollEvent

from .test_metadata_handling import get_cev


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
    test_depot.save_current()
    test_depot.current.purge()
    test_depot.init_w_complete()
    test_depot.current.integrate_events([get_cev(output_file="of-y")])
    test_depot.save_current()
    assert next(test_depot.get_handler_events()).cev.output_file == "of-y"


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
