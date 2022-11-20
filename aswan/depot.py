import os
import sys
import time
import zipfile
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from functools import partial, wraps
from hashlib import md5
from heapq import heappop, heappush
from itertools import islice
from pathlib import Path
from shutil import rmtree
from subprocess import CalledProcessError, check_output
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Callable, Dict, Iterable, List, Optional, Union

import sqlalchemy as db
import yaml
from sqlalchemy.orm import Session, sessionmaker

from .constants import (
    DEFAULT_DEPOT_ROOT,
    DEFAULT_REMOTE_ENV_VAR,
    DEPOT_ROOT_ENV_VAR,
    SUCCESS_STATUSES,
)
from .metadata_handling import get_next_batch, integrate_events, reset_surls
from .models import Base, CollEvent, RegEvent, partial_read, partial_read_path
from .object_store import ObjectStore
from .url_handler import ANY_HANDLER_T

if TYPE_CHECKING:  # pragma: no cover
    from fabric import Connection

HEX_ENV = "ASWAN_AUTH_HEX"
PW_ENV = "ASWAN_AUTH_PASS"

DB_KIND = "sqlite"  # :///
COMPRESS = zipfile.ZIP_DEFLATED
STATUS_DB_ZIP = f"db.{DB_KIND}.zip"
EVENTS_ZIP = "events.zip"
CONTEXT_YAML = "context.yaml"

_RUN_SPLIT = "-"

MySession = sessionmaker()


def _get_git_hash():
    # maybe as tag: git tag --sort=committerdate
    try:
        return check_output(["git", "rev-parse", "HEAD"]).decode("utf-8").strip()
    except CalledProcessError:
        return None


def _pip_freeze():
    comm = [sys.executable, "-m", "pip", "freeze"]
    return sorted(check_output(comm).decode("utf-8").strip().split("\n"))


def _hash_str(s: str):
    return md5(s.encode("utf-8")).hexdigest()[:20]


class _DepotObj:
    @classmethod
    def read(cls, dir_path: Path):
        return cls(**yaml.safe_load((dir_path / CONTEXT_YAML).read_text()))

    def dump(self, dir_path: Path):
        (dir_path / CONTEXT_YAML).write_text(yaml.dump(asdict(self)))


@dataclass
class Status(_DepotObj):
    parent: Optional[str] = None
    integrated_runs: List[str] = field(default_factory=list)

    def get_full_run_tree(self, status_finder: Callable[[str], "Status"]):
        out = set(self.integrated_runs)
        if self.parent:
            out |= status_finder(self.parent).get_full_run_tree(status_finder)
        return out

    @property
    def name(self):
        _run_str = "-".join(sorted(self.integrated_runs))
        return _hash_str(f"{self.parent}::{_run_str}")

    @property
    def is_root(self):
        return (self.parent is None) and (len(self.integrated_runs) == 0)


@dataclass
class Run(_DepotObj):
    commit_hash: str = field(default_factory=_get_git_hash)
    pip_freeze: List[str] = field(default_factory=_pip_freeze)
    start_timestamp: float = field(default_factory=time.time)


class Current:
    def __init__(self, root: Path) -> None:
        def _p(s) -> Path:
            return root / s

        self.root = root
        self.db_path, self.parent, self.events, self.run_ctx = map(
            _p, [f"db.{DB_KIND}", "parent", "events", CONTEXT_YAML]
        )
        self.db_constr = f"{DB_KIND}:///{self.db_path.as_posix()}"
        self.engine: db.engine.Engine = None
        self.next_batch = self._wrap(get_next_batch)
        self.reset_surls = self._wrap(reset_surls)

    def setup(self):
        self.engine = db.create_engine(self.db_constr)
        self.events.mkdir(parents=True, exist_ok=True)
        Base.metadata.create_all(self.engine)
        return self

    def purge(self):
        if self.root.exists():
            rmtree(self.root)

    def any_in_progress(self):
        # TODO
        pass

    def integrate_events(self, events: Iterable[Union[CollEvent, RegEvent]]):
        self._wrap(integrate_events)(events, self.events)

    def get_run_name(self):
        hash_base = self.run_ctx.read_text() + "::".join(
            sorted(map(Path.name.fget, self.events.iterdir()))
        )
        _ts = Run.read(self.root).start_timestamp
        return _RUN_SPLIT.join(map(str, [_ts, _hash_str(hash_base)]))

    @contextmanager
    def _get_session(self):
        session: Session = MySession(bind=self.engine)
        yield session
        session.close()

    def _wrap(self, fun):
        @wraps(fun)
        def f(*args, **kwargs):
            with self._get_session() as session:
                return fun(session, *args, **kwargs)

        return f


class AswanDepot:
    def __init__(self, name: str, local_root: Optional[Path] = None) -> None:
        self.name = name
        self.root = (
            Path(local_root or os.environ.get(DEPOT_ROOT_ENV_VAR) or DEFAULT_DEPOT_ROOT)
            / name
        )
        self.object_store_path = self.root / "object-store"
        self.object_store = ObjectStore(self.object_store_path)
        self.statuses_path = self.root / "statuses"
        self.runs_path = self.root / "runs"
        self.current = Current(self.root / "current-run")
        self._init_dirs = [self.runs_path, self.statuses_path, self.object_store_path]

    def setup(self, init=False):
        for p in self._init_dirs:
            p.mkdir(exist_ok=True, parents=True)
        if init:
            self.init_w_complete()
        return self

    def init_w_complete(self):
        self.set_as_current(self.get_complete_status())
        return self

    def get_complete_status(self) -> Status:
        # either an existing, a new or a blank status
        leaf = self._get_leaf()
        missing_runs = self.get_missing_runs(leaf)
        if missing_runs:
            return self.integrate(leaf, missing_runs)
        return leaf

    def get_status(self, status_name):
        return Status.read(self.statuses_path / status_name)

    def get_all_run_ids(self):
        return set(map(Path.name.fget, self.runs_path.iterdir()))

    def get_missing_runs(self, status: Status):
        return self.get_all_run_ids() - status.get_full_run_tree(self.get_status)

    def set_as_current(self, status: Status):
        self.current.setup()
        if not status.is_root:
            self.current.parent.write_text(status.name)
            with self._status_db_zip(status.name, "r") as zfp:
                zfp.extract(self.current.db_path.name, path=self.current.root)
        Run().dump(self.current.root)

    def integrate(self, status: Status, runs: Iterable[str]) -> Status:
        out = Status(status.name, list(runs))
        with TemporaryDirectory() as tmp_dir:
            tmp_curr = Current(Path(tmp_dir)).setup()
            with self._status_db_zip(status.name, "r") as zfp:
                zfp.extract(self.current.db_path.name, path=tmp_dir)
            for run_name in runs:
                tmp_curr.integrate_events(self._get_run_events(run_name, True))
            return self._save_status_from_current(tmp_curr, out)

    def save_current(self) -> Status:
        # not saving a zero event run!
        if not [*self.current.events.iterdir()]:
            return
        run_name = self.current.get_run_name()
        run_dir = self.runs_path / run_name
        run_dir.mkdir()
        with self._run_events_zip(run_name, "w") as zfp:
            for ev_path in self.current.events.iterdir():
                zfp.write(ev_path, ev_path.name)
        Run.read(self.current.root).dump(run_dir)

        try:
            parent = self.current.parent.read_text()
        except FileNotFoundError:
            parent = None
        status = Status(parent, [run_name])
        return self._save_status_from_current(self.current, status)

    def get_handler_events(
        self,
        handler: Optional[Union[str, ANY_HANDLER_T]] = None,
        only_successful=True,
        only_latest=True,
        from_current: bool = False,
        past_runs: Union[None, int, Iterable[str]] = None,
    ) -> Iterable["ParsedCollectionEvent"]:

        urls = set()
        handler_name = (
            handler
            if (isinstance(handler, str) or handler is None)
            else handler.__name__
        )

        def _filter(ev: CollEvent):
            return (
                ((handler_name is None) or (ev.handler == handler_name))
                and ((not only_successful) or (ev.status in SUCCESS_STATUSES))
                and ((not only_latest) or (ev.extend().url not in urls))
            )

        if from_current:
            event_iters = [map(partial_read_path, self.current.events.iterdir())]
        elif isinstance(past_runs, int):
            event_iters = islice(self._iter_runs(), past_runs)
        elif past_runs is None:
            event_iters = self._iter_runs()
        else:
            event_iters = map(self._get_run_events, sorted(past_runs, reverse=True))

        for ev_iter in event_iters:
            for ev in filter(_filter, get_sorted_coll_events(ev_iter)):
                yield ParsedCollectionEvent(ev, self.object_store)
                if only_latest:
                    urls.add(ev.url)

    def push(self, remote: Optional[str] = None):
        return self._conn_map(remote, self._push)

    def pull(self, remote: Optional[str] = None, complete=False, post_status=None):
        return self._conn_map(
            remote, self._pull, complete=complete, post_status=post_status
        )

    def purge(self):
        if self.root.exists():
            rmtree(self.root)
        return self

    def _get_leaf(self):
        by_name: Dict[str, Status] = {None: Status()}
        children: Dict[str, List[Status]] = defaultdict(list)
        for status in map(Status.read, self.statuses_path.iterdir()):
            by_name[status.name] = status
            children[status.parent].append(status)
        return _Chain(children, by_name).get_furthest_leaf(None)

    def _iter_runs(self) -> Iterable[Iterable[CollEvent]]:
        runs = []
        for run_path in self.runs_path.glob("*"):
            heappush(runs, (-_start_timestamp_from_run_path(run_path), run_path.name))
        while runs:
            _, run_name = heappop(runs)
            yield self._get_run_events(run_name)

    def _get_run_events(self, run_name, extend=False):
        with self._run_events_zip(run_name, "r") as zfp:
            for event in zfp.filelist:
                _fun = partial(_read_event_blob, self.runs_path, run_name, event)
                _ev = partial_read(event.filename, _fun)
                if extend:
                    _ev.extend()
                yield _ev

    def _save_status_from_current(self, current: Current, status: Status):
        status_dir = self.statuses_path / status.name
        status_dir.mkdir(parents=True)
        status.dump(status_dir)
        with self._status_db_zip(status.name, "w") as zfp:
            zfp.write(current.db_path, current.db_path.name)
        return status

    def _status_db_zip(self, status_name, mode):
        return _zipfile(self.statuses_path, status_name, STATUS_DB_ZIP, mode)

    def _run_events_zip(self, run_name, mode):
        return _zipfile(self.runs_path, run_name, EVENTS_ZIP, mode)

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
        remote_statuses = list(_ls(self.statuses_path))
        remote_runs = set(_ls(self.runs_path))
        for rem_status in remote_statuses:
            _mv(self.statuses_path / rem_status / CONTEXT_YAML)
        leaf = self._get_leaf()
        status_dbs_to_pull = set()
        if complete:
            status_dbs_to_pull = remote_statuses
        elif leaf.name in remote_statuses:
            status_dbs_to_pull.add(leaf.name)

        runs_to_pull = remote_runs - leaf.get_full_run_tree(self.get_status)
        if post_status is not None:
            break_status = self.get_status(post_status)
            runs_to_pull = remote_runs - break_status.get_full_run_tree(self.get_status)
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

    def _remote_ls(self, conn, dir_path: Path, only_remote=True) -> List[str]:
        import invoke

        local_posix = dir_path.relative_to(self.root).as_posix()
        try:
            _ls: List[str] = conn.run(f"ls {local_posix}", hide=True).stdout.split()
        except invoke.UnexpectedExit:
            _ls = []
        for remote_name in _ls:
            local_dir = dir_path / remote_name
            if only_remote and local_dir.exists():
                continue
            yield remote_name

    def _conn_move(self, conn: "Connection", local_path: Path):
        rem_abs_path = f"{conn.cwd}/{local_path.relative_to(self.root)}"
        if not local_path.exists():
            return conn.get(rem_abs_path, local_path.as_posix())


class ParsedCollectionEvent:
    def __init__(self, cev: "CollEvent", store: ObjectStore):
        self.cev = cev
        self.handler_name = cev.handler
        self._ostore = store
        self._time = cev.timestamp
        self.status = cev.status

    @property
    def content(self):
        self.cev.extend()
        of = self.cev.output_file
        return self._ostore.read(of) if of else None

    @property
    def url(self):
        self.cev.extend()
        return self.cev.url

    def __repr__(self):
        return f"{self.status}: {self.handler_name} - {self.url} ({self._time})"


def get_sorted_coll_events(event_iterator: Iterable) -> Iterable[CollEvent]:
    coll_evs = []
    for ev in event_iterator:
        if isinstance(ev, CollEvent):
            # ordered based on most recent
            heappush(coll_evs, ev)
    while coll_evs:
        yield heappop(coll_evs)


def get_remote(remote_name: str):
    from zimmauth import ZimmAuth

    return ZimmAuth.from_env(HEX_ENV, PW_ENV).get_fabric_connection(remote_name)


@dataclass
class _Chain:
    child_lists: Dict[str, List[Status]]
    nodes: Dict[str, Status]
    leaf_: Status = None
    max_dist_: int = 0

    def get_furthest_leaf(self, root_id, dist=0) -> Status:
        if dist >= self.max_dist_:
            self.max_dist_ = dist
            self.leaf_ = self.nodes[root_id]
        for child in self.child_lists[root_id]:
            self.get_furthest_leaf(child.name, dist + 1)
        return self.leaf_


def _read_event_blob(root, dirname, event_name):
    with _zipfile(root, dirname, EVENTS_ZIP, "r") as zfp:
        return zfp.read(event_name)


def _start_timestamp_from_run_path(p: Path):
    return float(p.name.split(_RUN_SPLIT)[0])


def _zipfile(root, dirname, filename, mode) -> zipfile.ZipFile:
    return zipfile.ZipFile(root / dirname / filename, mode, compression=COMPRESS)
