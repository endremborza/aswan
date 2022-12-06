import json

from aswan import AswanDepot
from aswan.monitor_app import MonitorApp

from .test_metadata_handling import get_cev


def test_monitor_app(test_depot: AswanDepot):

    test_depot.current.integrate_events([get_cev(), get_cev(timestamp=1200)])

    mapp = MonitorApp(test_depot)

    store = mapp.update_store(0, 10)
    assert json.dumps(store)
    elems = mapp.update_metrics(store)
    assert elems
    mapp.update_status(store)

    assert mapp.update_metrics({}) == []
