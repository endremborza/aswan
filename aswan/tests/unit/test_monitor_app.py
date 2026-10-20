import json
import sys

import pytest

from aswan import AswanDepot
from aswan.cli import main
from aswan.monitor_app import MonitorApp

from ..helpers import get_cev


def test_monitor_app(test_depot: AswanDepot):
    test_depot.current.integrate_events([get_cev(), get_cev(timestamp=1200)])

    mapp = MonitorApp(test_depot)

    store = mapp.update_store(0, 10)
    assert json.dumps(store)
    elems = mapp.update_metrics(store)
    assert elems
    mapp.update_status(store)

    assert mapp.update_metrics({**store, "coll_events": store["coll_events"][:1]})
    assert mapp.update_metrics({}) == []


def test_main_without_monitor_extra_names_the_extra(monkeypatch, tmp_path):
    monkeypatch.setitem(sys.modules, "dash", None)
    monkeypatch.setattr(
        sys, "argv", ["aswan", "monitor", "proj", "--root", str(tmp_path)]
    )
    with pytest.raises(SystemExit, match=r"aswan\[monitor\]"):
        main()
