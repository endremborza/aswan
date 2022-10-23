import os
from pathlib import Path

from aswan.depot import AswanDepot
from aswan.models import RegEvent


def test_push(env_auth_id: str, tmp_path: Path):
    if os.name == "nt":  # pragma: no cover
        return  # SSH needs to point to linux

    depot = AswanDepot("test", local_root=tmp_path / "depot1").setup(True)
    depot.current.integrate_events([RegEvent("url1", "H1")])
    depot.save_current()
    depot.push(env_auth_id)
    depot.push(env_auth_id)

    depot2 = AswanDepot("test", local_root=tmp_path / "depot2").setup(True)
    depot2.current.integrate_events([RegEvent("url2", "H1")])
    depot2.save_current()
    depot2.push(env_auth_id)

    depot.pull(env_auth_id)
    depot.current.purge()
    depot.set_as_current(depot.get_complete_status())

    assert sorted([su.url for su in depot.current.next_batch(3)]) == ["url1", "url2"]
