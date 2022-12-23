import os
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from ..constants import DEFAULT_REMOTE_ENV_VAR, HEX_ENV, PW_ENV
from .base import CONTEXT_YAML, EVENTS_ZIP, STATUS_DB_ZIP, DepotBase, StatusCache

if TYPE_CHECKING:  # pragma: no cover
    from fabric import Connection


class RemoteMixin(DepotBase):
    def push(self, remote: Optional[str] = None):
        return self._conn_map(remote, self._push)

    def pull(self, remote: Optional[str] = None, complete=False, post_status=None):
        return self._conn_map(
            remote, self._pull, complete=complete, post_status=post_status
        )

    def _conn_map(self, remote, fun, **kwargs):
        remote_name = (
            remote if isinstance(remote, str) else os.environ[DEFAULT_REMOTE_ENV_VAR]
        )
        with get_remote(remote_name) as conn:
            conn.run(f"mkdir -p {self.name}")
            with conn.cd(self.name):
                fun(conn, **kwargs)
        return self

    def _push(self, conn: "Connection"):
        present = set([fp[2:] for fp in conn.run("find .", hide=True).stdout.split()])
        for dir_path in self._init_dirs:
            for subdir in dir_path.iterdir():
                self._push_subdir(subdir, conn, present)
        self._status_cache.dump(self._cache_path)
        conn.put(
            self._cache_path.as_posix(),
            Path(conn.cwd, self._cache_path.name).as_posix(),
        )

    def _push_subdir(self, subdir: Path, conn: "Connection", present: set):
        rel_path = subdir.relative_to(self.root)
        if rel_path.as_posix() not in present:
            conn.run(f"mkdir -p {rel_path}")
        for elem in subdir.iterdir():
            rel_elem = rel_path / elem.name
            if rel_elem.as_posix() in present:
                continue
            rem_abs_path = f"{conn.cwd}/{rel_elem}"
            conn.put(elem.as_posix(), rem_abs_path)

    def _pull(self, conn: "Connection", complete: bool, post_status: Optional[str]):
        _ls = partial(self._remote_ls, conn)
        _mv = partial(self._conn_move, conn)
        self._merge_status_cache(conn)
        remote_statuses = set(_ls(self.statuses_path))
        if not complete:
            remote_statuses.difference_update(self._load_status_cache().statuses.keys())
        for rem_status in remote_statuses:
            _mv(self.statuses_path / rem_status / CONTEXT_YAML)
        leaf, leaf_tree = self._get_leaf()
        status_dbs_to_pull = set()
        if complete:
            status_dbs_to_pull = remote_statuses
        elif leaf.name in remote_statuses:
            status_dbs_to_pull.add(leaf.name)

        remote_runs = set(_ls(self.runs_path))
        runs_to_pull = remote_runs - leaf_tree
        if post_status is not None:
            break_status = self.get_status(post_status)
            runs_to_pull = remote_runs - self._get_full_run_tree(break_status)
        elif complete:
            runs_to_pull = remote_runs

        for status in status_dbs_to_pull:
            _mv(self.statuses_path / status / STATUS_DB_ZIP)
        for run in runs_to_pull:
            _mv(self.runs_path / run / EVENTS_ZIP)
        needed_objects = None
        if post_status is not None:
            pcevs = self.get_handler_events(only_latest=False, past_runs=runs_to_pull)
            needed_objects = set([pcev.cev.extend().output_file for pcev in pcevs])

        if (not complete) and (post_status is None):
            return

        for obj_dir in _ls(self.object_store_path, False):
            for obj_file in _ls(self.object_store_path / obj_dir):
                if (not complete) and (obj_file not in needed_objects):
                    continue
                _mv(self.object_store_path / obj_dir / obj_file)

    def _merge_status_cache(self, conn: "Connection") -> dict:
        import invoke

        tmp_path = self.root / "__rem.pkl"

        try:
            conn.run(f"test -f {self._cache_path.name}")
            rem_path = f"{conn.cwd}/{self._cache_path.name}"
            conn.get(rem_path, tmp_path.as_posix())
        except invoke.UnexpectedExit:
            pass

        rem_cache = StatusCache.read(tmp_path)
        tmp_path.unlink(missing_ok=True)
        self._status_cache.merge(rem_cache)

    def _remote_ls(self, conn, dir_path: Path, only_remote=True) -> list[str]:
        import invoke

        local_posix = dir_path.relative_to(self.root).as_posix()
        try:
            _ls: list[str] = conn.run(f"ls {local_posix}", hide=True).stdout.split()
        except invoke.UnexpectedExit:
            _ls = []
        local_set = set(dir_path.glob("*")) if dir_path.exists() else set()
        for remote_name in _ls:
            # local_dir = dir_path / remote_name
            if only_remote and ((dir_path / remote_name) in local_set):
                continue
            yield remote_name

    def _conn_move(self, conn: "Connection", local_path: Path):
        rem_abs_path = f"{conn.cwd}/{local_path.relative_to(self.root)}"
        if not local_path.exists():
            return conn.get(rem_abs_path, local_path.as_posix())


def get_remote(remote_name: str):
    from zimmauth import ZimmAuth

    return ZimmAuth.from_env(HEX_ENV, PW_ENV).get_fabric_connection(remote_name)
