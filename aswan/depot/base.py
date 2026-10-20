import json
import os
import time
import zipfile
from collections.abc import Iterable
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from functools import partial, wraps
from hashlib import md5
from heapq import heappop, heappush
from itertools import islice
from pathlib import Path
from shutil import copytree, rmtree
from subprocess import CalledProcessError, check_output
from tempfile import TemporaryDirectory

import sqlalchemy as db
from sqlalchemy.orm import Session, sessionmaker

from ..constants import (
    DEFAULT_DEPOT_ROOT,
    DEPOT_ROOT_ENV_VAR,
    SUCCESS_STATUSES,
    Statuses,
)
from ..metadata_handling import (
    get_grouped_surls,
    get_next_batch,
    integrate_events,
    reset_surls,
)
from ..models import Base, CollEvent, RegEvent, partial_read, partial_read_path
from ..object_store import ObjectStore
from ..url_handler import ANY_HANDLER_T
from . import sync

DB_KIND = "sqlite"
DB_FILE = f"db.{DB_KIND}"
COMPRESS = zipfile.ZIP_DEFLATED
CONTEXT_JSON = "context.json"
STATUS_DB_ZIP = "status.sqlite.zip"
STATUS_RUNS_JSON = "status-runs.json"
EVENTS_ZIP = "events.zip"

_RUN_SPLIT = "-"

MySession = sessionmaker()


def _get_git_hash() -> str | None:
    try:
        return check_output(["git", "rev-parse", "HEAD"]).decode("utf-8").strip()
    except (CalledProcessError, FileNotFoundError):
        return None


def _uv_freeze() -> list[str]:
    try:
        return check_output(["uv", "pip", "freeze"]).decode().strip().split("\n")
    except (CalledProcessError, FileNotFoundError):
        return []


def _hash_str(s: str) -> str:
    return md5(s.encode("utf-8")).hexdigest()[:20]


class _DepotObj:
    __path = CONTEXT_JSON

    @classmethod
    def read(cls, dir_path: Path):
        return cls(**json.loads((dir_path / cls.__path).read_text()))

    def dump(self, dir_path: Path) -> None:
        (dir_path / self.__path).write_text(json.dumps(asdict(self)))


@dataclass
class Status:
    integrated_runs: list[str] = field(default_factory=list)

    @property
    def name(self) -> str:
        return _hash_str("-".join(sorted(self.integrated_runs)))

    @property
    def is_root(self) -> bool:
        return len(self.integrated_runs) == 0


@dataclass
class Run(_DepotObj):
    commit_hash: str | None = field(default_factory=_get_git_hash)
    uv_freeze: list[str] = field(default_factory=_uv_freeze)
    start_timestamp: float = field(default_factory=time.time)
    is_test: bool = False


class Current:
    def __init__(self, root: Path) -> None:
        def _p(s) -> Path:
            return root / s

        self.root = root
        self.db_path, self.events, self.run_ctx = map(
            _p, [DB_FILE, "events", CONTEXT_JSON]
        )
        self.db_constr = f"{DB_KIND}:///{self.db_path.as_posix()}"
        self.events.mkdir(parents=True, exist_ok=True)
        self.engine = db.create_engine(self.db_constr)
        self.next_batch = self._wrap(get_next_batch)
        self.reset_surls = self._wrap(reset_surls)

    def setup(self) -> "Current":
        self.events.mkdir(parents=True, exist_ok=True)
        # Pooled connections may point at a db file that has since been replaced
        # (purge, status extract), so they are released before reconnecting.
        self.engine.dispose()
        self.engine = db.create_engine(self.db_constr)
        Base.metadata.create_all(self.engine)
        return self

    def purge(self) -> None:
        if self.root.exists():
            rmtree(self.root)

    def any_in_progress(self) -> bool:
        with self._get_session() as session:
            for status, _, count in get_grouped_surls(session):
                if status == Statuses.PROCESSING:
                    return count > 0
        return False

    def integrate_events(self, events: Iterable[CollEvent | RegEvent]) -> None:
        self._wrap(integrate_events)(events, self.events)

    def apply_events(self, events: Iterable[CollEvent | RegEvent]) -> None:
        """Update SQLite from events already dumped to disk by a worker."""
        self._wrap(integrate_events)(events, None)

    def is_test(self) -> bool:
        return self.run_ctx.exists() and Run.read(self.root).is_test

    def get_run_name(self) -> str:
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


def depot_root(local_root: Path | str | None = None) -> Path:
    """The directory holding depots: ``local_root``, else the env var, else the default."""
    return Path(local_root or os.environ.get(DEPOT_ROOT_ENV_VAR) or DEFAULT_DEPOT_ROOT)


class AswanDepot:
    def __init__(self, name: str, local_root: Path | str | None = None) -> None:
        self.name = name
        self.root = depot_root(local_root) / name
        self.object_store_path = self.root / "object-store"
        self.object_store = ObjectStore(self.object_store_path)
        self.runs_path = self.root / "runs"
        self.current = Current(self.root / "current-run")
        self.status_db = self.root / STATUS_DB_ZIP
        self.status_runs = self.root / STATUS_RUNS_JSON

    def setup(self, init: bool = False) -> "AswanDepot":
        self.runs_path.mkdir(exist_ok=True, parents=True)
        if init:
            self.init_w_complete()
        return self

    def purge(self) -> "AswanDepot":
        if self.root.exists():
            rmtree(self.root)
        return self

    def init_w_complete(self) -> "AswanDepot":
        self.set_as_current(self.get_complete_status())
        return self

    def get_complete_status(self) -> Status:
        integrated = self._integrated_runs()
        missing = self.get_all_run_ids() - integrated
        if not missing:
            return Status(integrated_runs=sorted(integrated))

        with TemporaryDirectory() as tmp_dir:
            tmp_curr = Current(Path(tmp_dir))
            if integrated:
                self._extract_status_db(tmp_curr.root)
            tmp_curr.setup()
            for run_name in sorted(missing, key=_run_start_ts):
                tmp_curr.integrate_events(self._get_run_events(run_name, True))
            return self._write_status(tmp_curr.db_path, integrated | missing)

    def get_all_run_ids(self) -> set[str]:
        if not self.runs_path.exists():
            return set()
        return set(map(Path.name.fget, self.runs_path.iterdir()))

    def set_as_current(self, status: Status, is_test: bool = False) -> None:
        if self.current.is_test():
            self.current.purge()
        self.current.setup()
        if not status.is_root and self.status_db.exists():
            self._extract_status_db(self.current.root)
        Run(is_test=is_test).dump(self.current.root)

    def save_current(self) -> "Status | None":
        if self.current.is_test():
            raise PermissionError("the current run is a test run, it is never saved")
        if not any(self.current.events.iterdir()):
            return None
        run_name = self.current.get_run_name()
        run_dir = self.runs_path / run_name
        run_dir.mkdir()
        with _zipfile_open(run_dir / EVENTS_ZIP, "w") as zf:
            for ev_path in self.current.events.iterdir():
                zf.write(ev_path, ev_path.name)
        Run.read(self.current.root).dump(run_dir)
        self.object_store.commit()
        return self._write_status(
            self.current.db_path, self._integrated_runs() | {run_name}
        )

    def get_handler_events(
        self,
        handler: "str | type[ANY_HANDLER_T] | None" = None,
        only_successful: bool = True,
        only_latest: bool = True,
        from_current: bool = False,
        past_runs: "None | int | Iterable[str]" = None,
        post_status: "Status | None" = None,
    ) -> "Iterable[ParsedCollectionEvent]":
        urls: set[str] = set()
        handler_name = (
            handler
            if (isinstance(handler, str) or handler is None)
            else handler.__name__
        )

        def _filter(ev: CollEvent) -> bool:
            return (
                ((handler_name is None) or (ev.handler == handler_name))
                and ((not only_successful) or (ev.status in SUCCESS_STATUSES))
                and ((not only_latest) or (ev.extend().url not in urls))
            )

        if post_status is not None:
            past_runs = self.get_all_run_ids() - set(post_status.integrated_runs)

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

    def merge(self, other: "AswanDepot") -> None:
        """Merge another depot's runs and objects into this depot in-place."""
        for run_dir in other.runs_path.iterdir():
            dest = self.runs_path / run_dir.name
            if not dest.exists():
                copytree(run_dir, dest)
        self.object_store.merge_from(other.object_store)
        self.status_runs.unlink(missing_ok=True)
        self.status_db.unlink(missing_ok=True)

    def push(self, remote: str | None = None) -> None:
        """Send runs, status and committed objects to `<remote>/<name>`.

        `remote` is anything rsync accepts (a path or `user@host:/path`) and
        defaults to the `REMOTE_ENV_VAR` environment variable.
        """
        sync.push(self, remote)

    def pull(self, remote: str | None = None) -> None:
        """Fetch what `<remote>/<name>` has that this depot lacks; a no-op if absent."""
        sync.pull(self, remote)

    def _integrated_runs(self) -> set[str]:
        """Runs folded into the status db; none when the db itself is missing."""
        if not (self.status_db.exists() and self.status_runs.exists()):
            return set()
        return set(json.loads(self.status_runs.read_text()))

    def _extract_status_db(self, dest_dir: Path) -> None:
        with _zipfile_open(self.status_db, "r") as zf:
            zf.extract(DB_FILE, dest_dir)

    def _write_status(self, db_path: Path, runs: set[str]) -> Status:
        with _zipfile_open(self.status_db, "w") as zf:
            zf.write(db_path, DB_FILE)
        self.status_runs.write_text(json.dumps(sorted(runs)))
        return Status(integrated_runs=sorted(runs))

    def _iter_runs(self) -> "Iterable[Iterable[CollEvent]]":
        runs: list = []
        for run_path in self.runs_path.glob("*"):
            heappush(runs, (-_run_start_ts(run_path.name), run_path.name))
        while runs:
            _, run_name = heappop(runs)
            yield self._get_run_events(run_name)

    def _get_run_events(self, run_name: str, extend: bool = True):
        with _zipfile_open(self.runs_path / run_name / EVENTS_ZIP, "r") as zf:
            for event in zf.filelist:
                if extend:
                    yield partial_read(event.filename, partial(zf.read, event)).extend()
                else:
                    _fun = partial(_read_event_blob, self.runs_path, run_name, event)
                    yield partial_read(event.filename, _fun)


class ParsedCollectionEvent:
    def __init__(self, cev: "CollEvent", store: ObjectStore) -> None:
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
    def url(self) -> str:
        self.cev.extend()
        return self.cev.url

    def __repr__(self) -> str:
        return f"{self.status}: {self.handler_name} - {self.url} ({self._time})"


def get_sorted_coll_events(event_iterator: Iterable) -> "Iterable[CollEvent]":
    coll_evs: list = []
    for ev in event_iterator:
        if isinstance(ev, CollEvent):
            heappush(coll_evs, ev)
    while coll_evs:
        yield heappop(coll_evs)


def _read_event_blob(root: Path, dirname: str, event_name) -> bytes:
    with _zipfile_open(root / dirname / EVENTS_ZIP, "r") as zf:
        return zf.read(event_name)


def _run_start_ts(run_name: str) -> float:
    return float(run_name.split(_RUN_SPLIT)[0])


def _zipfile_open(path: Path, mode: str) -> zipfile.ZipFile:
    return zipfile.ZipFile(path, mode, compression=COMPRESS)
