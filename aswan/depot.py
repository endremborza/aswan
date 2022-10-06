import sys
import time
import zipfile
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from functools import partial, wraps
from hashlib import md5
from heapq import heappop, heappush
from itertools import chain, islice
from pathlib import Path
from shutil import rmtree
from subprocess import CalledProcessError, check_output
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Union

import sqlalchemy as db
import yaml
from sqlalchemy.orm import Session, sessionmaker

from .constants import Statuses
from .metadata_handling import get_next_batch, integrate_events, reset_surls
from .models import Base, CollEvent, RegEvent, partial_read, partial_read_path

if TYPE_CHECKING:
    from .connection_session import UrlHandlerResult  # pragma: no cover

DEFAULT_LOCAL_SPACE = Path.home() / "aswan-depots"
DB_KIND = "sqlite"  # :///

COMPRESS = zipfile.ZIP_DEFLATED
STATUS_DB_ZIP = f"db.{DB_KIND}.zip"
CONTEXT_YAML = "context.yaml"

_RUN_SPLIT = "-"

MySession = sessionmaker()


def _get_git_hash():
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
        self.engine = db.create_engine(self.db_constr)
        self.next_batch = self._wrap(get_next_batch)
        self.reset_surls = self._wrap(reset_surls)

    def setup(self):
        self.events.mkdir(parents=True)
        Base.metadata.create_all(self.engine)

    def purge(self):
        if self.root.exists():
            rmtree(self.root)

    def any_in_progress(self):
        # TODO
        pass

    def process_results(self, result_queue: Iterable["UrlHandlerResult"]):
        all_events = []
        for uh_result in result_queue:
            all_events += [uh_result.event, *uh_result.registered_links]
        self.integrate_events(all_events)

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
    def __init__(self, name: str, local_root: Optional[Path]) -> None:
        self.root = Path(local_root or DEFAULT_LOCAL_SPACE) / name
        self.object_store_path = self.root / "object-store"
        self.statuses_path = self.root / "statuses"
        self.runs_path = self.root / "runs"
        self.current = Current(self.root / "current-run")

    def setup(self):
        for p in [self.runs_path, self.statuses_path, self.object_store_path]:
            p.mkdir(exist_ok=True, parents=True)

    def get_complete_status(self) -> Status:
        # either an existing, a new or a blank status
        by_name: Dict[str, Status] = {None: Status()}
        children: Dict[str, List[Status]] = defaultdict(list)
        for status in map(Status.read, self.statuses_path.iterdir()):
            by_name[status.name] = status
            children[status.parent].append(status)
        leaf = _Chain(children, by_name).get_furthest_leaf(None)
        missing_runs = self.get_all_run_ids() - set(leaf.integrated_runs)
        if missing_runs:
            return self.integrate(leaf, missing_runs)
        return leaf

    def get_all_run_ids(self):
        return set(map(Path.name.fget, self.runs_path.iterdir()))

    def set_as_current(self, status: Status):
        self.current.setup()
        if not status.is_root:
            self.current.parent.write_text(status.name)
            with self._status_db_zip(status.name, "r") as zfp:
                zfp.extract(self.current.db_path.name, path=self.current.root)
        Run().dump(self.current.root)

    def integrate(self, status: Status, runs: Iterable[str]):
        out = Status(status.name, list(runs))
        with TemporaryDirectory() as tmp_dir:
            tmp_curr = Current(tmp_dir)
            for run_name in runs:
                tmp_curr.integrate_events(self._get_run_events(run_name))
            self._save_status_from_current(tmp_curr, out)
        return out

    def save_current(self):
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
        self._save_status_from_current(self.current, status)

    def get_handler_events(
        self,
        handler: Optional[str] = None,
        only_successful=True,
        only_latest=True,
        limit=float("inf"),
        past_runs=0,
    ) -> Iterable[CollEvent]:
        n = 0
        urls = set()

        def _filter(ev: CollEvent):
            return (
                ((handler is None) or (ev.handler == handler))
                and ((not only_successful) or (ev.status == Statuses.PROCESSED))
                and ((not only_latest) or (ev.extend().url not in urls))
            )

        if past_runs > 0:
            event_iters = islice(self._iter_runs(), past_runs)
        else:
            event_iters = [map(partial_read_path, self.current.events.iterdir())]
        for ev_iter in chain(event_iters):
            for ev in filter(_filter, get_sorted_coll_events(ev_iter)):
                if n > limit:
                    break
                yield ev
                if only_latest:
                    urls.add(ev.url)
                n += 1

    def purge(self):
        if self.root.exists():
            rmtree(self.root)

    def _iter_runs(self) -> Iterable[Iterable[CollEvent]]:
        runs = []
        for run_path in self.runs_path.glob("*"):
            heappush(runs, (_start_timestamp_from_run_path(run_path), run_path.name))
        while runs:
            _, run_name = heappop(runs)
            yield self._get_run_events(run_name)

    def _get_run_events(self, run_name):
        with self._run_events_zip(run_name, "r") as zfp:
            for event in zfp.filelist:
                yield partial_read(
                    event.filename, partial(self._read_event_blob, run_name, event)
                )

    def _read_event_blob(self, run_name, event_name):
        with self._run_events_zip(run_name, "r") as zfp:
            return zfp.read(event_name)

    def _save_status_from_current(self, current: Current, status: Status):
        status_dir = self.statuses_path / status.name
        status_dir.mkdir(parents=True)
        status.dump(status_dir)
        with self._status_db_zip(status.name, "w") as zfp:
            zfp.write(current.db_path, current.db_path.name)

    def _status_db_zip(self, status_name, mode):
        return _zipfile(self.statuses_path, status_name, STATUS_DB_ZIP, mode)

    def _run_events_zip(self, run_name, mode):
        return _zipfile(self.runs_path, run_name, "events.zip", mode)


def get_sorted_coll_events(event_iterator: Iterable) -> Iterable[CollEvent]:
    coll_evs = []
    for ev in event_iterator:
        if isinstance(ev, CollEvent):
            # ordered based on most recent
            heappush(coll_evs, ev)
    while coll_evs:
        yield heappop(coll_evs)


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


def _start_timestamp_from_run_path(p: Path):
    return float(p.name.split(_RUN_SPLIT)[0])


def _zipfile(root, dirname, filename, mode) -> zipfile.ZipFile:
    return zipfile.ZipFile(root / dirname / filename, mode, compression=COMPRESS)