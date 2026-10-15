"""Depot push/pull via rsync to a local directory standing in for a remote."""

import shutil

import pytest

import aswan
from aswan.constants import REMOTE_ENV_VAR

from ..helpers import seed_run


@pytest.fixture
def remote_root(tmp_path):
    if shutil.which("rsync") is None:
        pytest.skip("rsync not installed")
    r = tmp_path / "remote"
    r.mkdir()
    return r


def test_env_remote_roundtrip(tmp_path, remote_root, monkeypatch):
    monkeypatch.setenv(REMOTE_ENV_VAR, str(remote_root))
    depot = aswan.AswanDepot("ENV", tmp_path / "a").setup(init=True)
    seed_run(depot, output_file=depot.object_store.dump_str("payload"))
    depot.push()

    pulled = aswan.AswanDepot("ENV", tmp_path / "b")
    pulled.pull()
    assert [e.content for e in pulled.get_handler_events()] == ["payload"]


def test_remote_unset_raises(tmp_path, monkeypatch):
    monkeypatch.delenv(REMOTE_ENV_VAR, raising=False)
    depot = aswan.AswanDepot("NX", tmp_path / "a")
    with pytest.raises(ValueError, match=REMOTE_ENV_VAR):
        depot.push()
    with pytest.raises(ValueError, match=REMOTE_ENV_VAR):
        depot.pull()


def test_uncommitted_work_stays_local(tmp_path, remote_root):
    depot = aswan.AswanDepot("EXCL", tmp_path / "a").setup(init=True)
    seed_run(depot)
    staged = depot.object_store.dump_str("staged, never committed")
    depot.push(str(remote_root))

    assert not (remote_root / depot.name / depot.current.root.name).exists()
    pulled = aswan.AswanDepot("EXCL", tmp_path / "b")
    pulled.pull(str(remote_root))
    assert pulled.get_all_run_ids() == depot.get_all_run_ids()
    with pytest.raises(KeyError):
        pulled.object_store.read(staged)
