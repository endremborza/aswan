"""Build a synthetic pre-1.0 depot and exercise the legacy converter."""

import pickle

import pytest

import aswan
from aswan.depot.base import Run
from aswan.legacy import convert_depot

yaml = pytest.importorskip("yaml")


def _make_legacy_depot(root):
    """Construct a depot directory in the old YAML-context + DAG-statuses
    layout. Mirrors what `aswan==0.5.x` would have written.
    """
    root.mkdir()
    (root / "object-store").mkdir()
    (root / "runs").mkdir()
    (root / "statuses").mkdir()
    (root / "current-run").mkdir()
    (root / "current-run" / "events").mkdir()

    (root / "current-run" / "context.yaml").write_text(
        yaml.dump(
            {
                "commit_hash": "deadbeef",
                "pip_freeze": ["requests==2.0"],
                "start_timestamp": 1.0,
            }
        )
    )
    run_dir = root / "runs" / "1000-aaaaa"
    run_dir.mkdir()
    (run_dir / "context.yaml").write_text(
        yaml.dump(
            {
                "commit_hash": "feedface",
                "pip_freeze": ["bs4"],
                "start_timestamp": 1000.0,
            }
        )
    )
    # DAG bookkeeping that legacy.convert_depot should strip
    (root / "status-cache.pkl").write_bytes(pickle.dumps({"obsolete": True}))
    sub = root / "statuses" / "root"
    sub.mkdir()
    (sub / "context.yaml").write_text(yaml.dump({"parent": None}))
    return root


def test_convert_in_place(tmp_path):
    legacy = _make_legacy_depot(tmp_path / "old")
    out = convert_depot(legacy)

    assert out == legacy
    assert (legacy.parent / "old.bak").is_dir()
    assert (legacy / "current-run" / "context.json").exists()
    assert not (legacy / "current-run" / "context.yaml").exists()
    assert not (legacy / "status-cache.pkl").exists()
    assert not (legacy / "statuses").exists()

    run_dir = legacy / "runs" / "1000-aaaaa"
    assert (run_dir / "context.json").exists()


def test_convert_to_new_path(tmp_path):
    legacy = _make_legacy_depot(tmp_path / "src")
    dest = tmp_path / "dst"
    out = convert_depot(legacy, dest)

    assert out == dest
    assert (dest / "current-run" / "context.json").exists()
    assert legacy.exists(), "source should remain untouched when output_path is given"
    assert (legacy / "current-run" / "context.yaml").exists()


def test_converted_depot_is_readable(tmp_path):
    legacy = _make_legacy_depot(tmp_path / "src")
    dest = tmp_path / "converted"
    convert_depot(legacy, dest)

    depot = aswan.AswanDepot("converted", tmp_path)
    runs = depot.get_all_run_ids()
    assert "1000-aaaaa" in runs


def test_converted_contexts_load_as_runs(tmp_path):
    converted = convert_depot(_make_legacy_depot(tmp_path / "src"), tmp_path / "dst")
    assert Run.read(converted / "current-run").uv_freeze == ["requests==2.0"]
    assert Run.read(converted / "runs" / "1000-aaaaa").uv_freeze == ["bs4"]
