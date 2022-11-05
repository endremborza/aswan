import os
from pathlib import Path

import aswan
from aswan.constants import Statuses
from aswan.depot import Run, Status
from aswan.models import CollEvent


def test_coll_event_handling(test_project2: aswan.Project):

    depot = test_project2.depot
    depot.setup()
    depot.set_as_current(Status())

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

    depot.current.integrate_events(coll_events)

    cevs1 = [*depot.get_handler_events(At, only_latest=True, only_successful=True)]

    assert len(cevs1) == 1
    assert cevs1[0].cev.extend().output_file == "of2"

    cevs2 = [*depot.get_handler_events(At, only_latest=False, only_successful=True)]
    assert len(cevs2) == 2
    assert set([cev.cev.extend().output_file for cev in cevs2]) == {"of2", "of1"}

    cevs3 = [*depot.get_handler_events(Bt, only_latest=True, only_successful=False)]
    assert len(cevs3) == 2
    assert set([cev.cev.extend().output_file for cev in cevs3]) == {"of4", "of6"}

    cevs4 = [*depot.get_handler_events(Bt, only_latest=False, only_successful=False)]
    assert len(cevs4) == 3
    assert set([cev.cev.extend().output_file for cev in cevs4]) == {"of4", "of5", "of6"}


def test_depobj_init(tmp_path):
    wd = Path.cwd()
    os.chdir(tmp_path)
    try:
        Run()
    finally:
        os.chdir(wd)
