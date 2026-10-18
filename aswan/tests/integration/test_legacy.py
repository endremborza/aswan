"""Migrate synthetic pre-1.0 depots, built in the layout `aswan==0.5.x` wrote."""

import hashlib
import pickle
import zipfile
from pathlib import Path

import pytest

from aswan import AswanDepot
from aswan.constants import Statuses
from aswan.depot.base import EVENTS_ZIP, Run
from aswan.legacy import migrate_depot
from aswan.models import CollEvent, RegEvent

from ..helpers import statuses

yaml = pytest.importorskip("yaml")

_Pages = dict[str, bytes | None]


def _context(ts: float) -> str:
    return yaml.dump(
        {"commit_hash": "feedface", "pip_freeze": ["bs4"], "start_timestamp": ts}
    )


def _events(
    store: Path, pages: _Pages, ts: int, status: str
) -> list[tuple[str, bytes]]:
    out = []
    for url, content in pages.items():
        name = ""
        if content is not None:
            name = f"{hashlib.sha3_512(content).hexdigest()}.blob"
            path = store / name[:2] / name
            path.parent.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("content", content)
        out.append(RegEvent(url, "H").encode())
        out.append(CollEvent(url, "H", status, ts, name).encode())
    return out


def _legacy_depot(
    root: Path,
    runs: dict[str, _Pages],
    current: _Pages | None = None,
    status: str = Statuses.PROCESSED,
) -> Path:
    store = root / "object-store"
    store.mkdir(parents=True)
    (root / "statuses" / "root").mkdir(parents=True)
    (root / "status-cache.pkl").write_bytes(pickle.dumps({"obsolete": True}))
    for run_id, pages in runs.items():
        ts = float(run_id.split("-")[0])
        run_dir = root / "runs" / run_id
        run_dir.mkdir(parents=True)
        (run_dir / "context.yaml").write_text(_context(ts))
        with zipfile.ZipFile(run_dir / EVENTS_ZIP, "w") as zf:
            for name, blob in _events(store, pages, int(ts), status):
                zf.writestr(name, blob)
    if current is not None:
        events = root / "current-run" / "events"
        events.mkdir(parents=True)
        (root / "current-run" / "context.yaml").write_text(_context(3000.0))
        for name, blob in _events(store, current, 3000, status):
            (events / name).write_bytes(blob)
    return root


def _contents(depot: AswanDepot) -> dict[str, bytes | None]:
    evs = depot.get_handler_events("H", only_latest=False)
    return {ev.url: ev.content for ev in evs}


def test_runs_objects_and_current_run(tmp_path):
    legacy = _legacy_depot(
        tmp_path / "old",
        {
            "1000.0-aaaa": {"u1": b"page one", "u2": None},
            "2000.0-bbbb": {"u3": b"page one"},
        },
        current={"u4": b"page four"},
    )
    (legacy / "runs" / "2000.0-bbbb" / "context.yaml").unlink()
    target = AswanDepot("new", tmp_path)

    report = migrate_depot(legacy, target)

    assert (report.runs, report.present, report.objects) == (3, 0, 2)
    assert not report.broken
    run_ids = target.get_all_run_ids()
    assert {"1000.0-aaaa", "2000.0-bbbb"} < run_ids and len(run_ids) == 3
    assert _contents(target) == {
        "u1": b"page one",
        "u2": None,
        "u3": b"page one",
        "u4": b"page four",
    }
    assert set(statuses(target).values()) == {Statuses.PROCESSED}
    assert Run.read(target.runs_path / "1000.0-aaaa").uv_freeze == ["bs4"]
    assert Run.read(target.runs_path / "2000.0-bbbb").start_timestamp == 2000.0
    assert not (target.root / "statuses").exists()


def test_copies_merge_and_rerun_skips(tmp_path):
    old = _legacy_depot(tmp_path / "old", {"1000.0-aaaa": {"u1": b"one"}})
    newer = _legacy_depot(
        tmp_path / "newer",
        {"1000.0-aaaa": {"u1": b"one"}, "2000.0-bbbb": {"u2": b"two"}},
    )
    target = AswanDepot("new", tmp_path)

    migrate_depot(old, target)
    report = migrate_depot(newer, target)
    again = migrate_depot(newer, target)

    assert (report.runs, report.present) == (1, 1)
    assert (again.runs, again.present, again.objects) == (0, 2, 0)
    assert _contents(target) == {"u1": b"one", "u2": b"two"}


def test_missing_object_holds_back_its_run(tmp_path):
    pages = {"1000.0-aaaa": {"u1": b"one"}, "2000.0-bbbb": {"u2": b"two"}}
    broken = _legacy_depot(tmp_path / "broken", pages)
    lost = hashlib.sha3_512(b"two").hexdigest()
    (broken / "object-store" / lost[:2] / f"{lost}.blob").unlink()
    target = AswanDepot("new", tmp_path)

    report = migrate_depot(broken, target)
    assert target.get_all_run_ids() == {"1000.0-aaaa"}
    assert list(report.broken) == ["2000.0-bbbb"]

    migrate_depot(_legacy_depot(tmp_path / "whole", pages), target)
    assert _contents(target) == {"u1": b"one", "u2": b"two"}


def test_persistent_statuses_become_plain(tmp_path):
    pages = {"u1": b"one", "u2": None}
    legacy = _legacy_depot(tmp_path / "old", {"1000.0-aaaa": pages}, status="PP")
    target = AswanDepot("new", tmp_path)

    migrate_depot(legacy, target)

    assert _contents(target) == pages
    assert set(statuses(target).values()) == {Statuses.PROCESSED}
