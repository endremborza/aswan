"""Migrate a pre-1.0 depot into a current one.

A legacy depot stores each object as a zip file holding one `content` member
at `object-store/<name[:2]>/<name>`, run contexts as YAML (or none) and a DAG of
statuses. `migrate_depot` re-stores every object an event names in the
target's `ObjectStore`, rewrites the event's `output_file` to the new name and
writes the run under its legacy id with a JSON context. The persistent
statuses 1.0 dropped become their plain counterparts. The status DAG is left
behind: `get_complete_status` rebuilds status from the runs. The events of a
legacy `current-run` become one more run.

Run ids the target already holds are skipped, so the same call pulls new runs
from a live legacy depot, merges several copies of one depot into one target
and resumes an interrupted migration. A run with a missing or unreadable
object, or with an event the current model cannot represent, is not written:
it is reported, and another copy can supply it.
"""

from __future__ import annotations

import json
import logging
import shutil
import zipfile
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path

from dirark import blob_checksum

from .constants import Statuses
from .depot.base import (
    CONTEXT_JSON,
    EVENTS_ZIP,
    AswanDepot,
    Run,
    _hash_str,
    _run_start_ts,
    _zipfile_open,
)
from .models import BLOB_JOIN, CollEvent, partial_read

log = logging.getLogger(__name__)

_LEGACY_CONTEXT = "context.yaml"
_CONTENT = "content"
_RENAMED_KEYS = {"pip_freeze": "uv_freeze"}
_RENAMED_STATUSES = {"PP": Statuses.PROCESSED, "PC": Statuses.CACHE_LOADED}
_COMMIT_BYTES = 256 << 20
_STAGE = ".migrating"
_READERS = 16
_READ_BATCH = 4096

_Events = list[tuple[str, bytes]]


@dataclass
class Report:
    runs: int = 0
    present: int = 0
    events: int = 0
    objects: int = 0
    broken: dict[str, list[str]] = field(default_factory=dict)

    def __str__(self) -> str:
        lines = [
            f"{self.runs} runs, {self.events} events, {self.objects} objects"
            f" migrated; {self.present} runs already in the target"
        ]
        if self.broken:
            lines.append(f"{len(self.broken)} runs not written:")
            for run_id, problems in self.broken.items():
                lines.append(f"  {run_id}: {len(problems)}, first {problems[0]}")
        return "\n".join(lines)


@dataclass
class _LegacyRun:
    id: str
    context: dict
    events: Callable[[], Iterator[tuple[str, bytes]]]


class _Objects:
    """Legacy object name -> name in the target's store.

    A legacy store is one small file per object, often on a network mount, so
    `restore` reads in parallel; writes stay on the calling thread. `staged`
    counts the bytes new to the target since the last commit, so merging a copy
    whose objects the target already holds packs nothing."""

    def __init__(self, root: Path, target: AswanDepot, pool: ThreadPoolExecutor):
        self.root = root
        self.store = target.object_store
        self.pool = pool
        self.names: dict[str, str] = {}
        self.staged = 0

    def restore(self, names: Iterable[str]) -> None:
        todo = [n for n in dict.fromkeys(names) if n not in self.names]
        for i in range(0, len(todo), _READ_BATCH):
            batch = todo[i : i + _READ_BATCH]
            paths = [self.root / n[:2] / n for n in batch]
            for name, buf in zip(batch, self.pool.map(_read_legacy, paths)):
                if buf is not None:
                    self._store(name, buf)

    def _store(self, name: str, buf: bytes) -> None:
        ext = name.rsplit(".", 1)[-1]
        if not self.store.has(f"{blob_checksum(buf)}.{ext}"):
            self.staged += len(buf)
        self.names[name] = self.store.dump_bytes(buf, ext)

    def commit(self) -> None:
        self.store.commit()
        self.staged = 0


def migrate_depot(legacy: Path, target: AswanDepot) -> Report:
    """Copy the runs of the legacy depot at `legacy` that `target` lacks."""
    target.setup()
    stage = target.root / _STAGE
    shutil.rmtree(stage, ignore_errors=True)
    stage.mkdir()
    present = target.get_all_run_ids()
    report = Report()
    with ThreadPoolExecutor(_READERS) as pool:
        objects = _Objects(legacy / "object-store", target, pool)
        for run in _legacy_runs(legacy):
            if run.id in present:
                report.present += 1
                continue
            events, problems = _convert(run, objects)
            if problems:
                report.broken[run.id] = problems
            else:
                _write_run(target, stage / run.id, run, events)
                report.runs += 1
                report.events += len(events)
            if objects.staged >= _COMMIT_BYTES:
                objects.commit()
                log.info("%d runs, %d objects", report.runs, len(objects.names))
    objects.commit()
    stage.rmdir()
    report.objects = len(objects.names)
    return report


def _legacy_runs(legacy: Path) -> Iterator[_LegacyRun]:
    runs = legacy / "runs"
    for run_dir in sorted(runs.iterdir()) if runs.exists() else []:
        events = partial(_zipped_events, run_dir / EVENTS_ZIP)
        yield _LegacyRun(run_dir.name, _context(run_dir, run_dir.name), events)
    current = legacy / "current-run"
    loose = current / "events"
    names = sorted(p.name for p in loose.iterdir()) if loose.exists() else []
    if names:
        context = _context(current)
        base = json.dumps(context, sort_keys=True) + "::".join(names)
        run_id = f"{context['start_timestamp']}-{_hash_str(base)}"
        yield _LegacyRun(run_id, context, partial(_loose_events, loose, names))


def _convert(run: _LegacyRun, objects: _Objects) -> tuple[_Events, list[str]]:
    try:
        raw = list(run.events())
    except (FileNotFoundError, zipfile.BadZipFile) as e:
        return [], [f"events: {e}"]
    blob_keys = len(list(CollEvent._blob_keys()))
    problems: list[str] = []
    rewrite: dict[str, CollEvent] = {}
    for name, blob in raw:
        ev = partial_read(name, lambda b=blob: b)
        if not isinstance(ev, CollEvent):
            continue
        if len(blob.split(BLOB_JOIN)) != blob_keys:
            problems.append(f"event {name}")
        elif ev.extend().output_file or ev.status in _RENAMED_STATUSES:
            ev.status = _RENAMED_STATUSES.get(ev.status, ev.status)
            rewrite[name] = ev
    # in the order aswan reads them, so a run's objects share tars
    by_recency = sorted(rewrite.values(), key=lambda ev: ev.recency)
    objects.restore(ev.output_file for ev in by_recency if ev.output_file)
    events: _Events = []
    for name, blob in raw:
        if (ev := rewrite.get(name)) is None:
            events.append((name, blob))
            continue
        if ev.output_file:
            if (new := objects.names.get(ev.output_file)) is None:
                problems.append(f"object {ev.output_file}")
                continue
            ev.output_file = new
        events.append(ev.encode())
    return events, problems


def _write_run(target: AswanDepot, tmp: Path, run: _LegacyRun, events: _Events):
    tmp.mkdir()
    with _zipfile_open(tmp / EVENTS_ZIP, "w") as zf:
        for name, blob in events:
            zf.writestr(name, blob)
    Run(**run.context).dump(tmp)
    tmp.rename(target.runs_path / run.id)


def _context(run_dir: Path, run_id: str | None = None) -> dict:
    """The run's context; a legacy run without one gets its start from `run_id`."""
    legacy, current = run_dir / _LEGACY_CONTEXT, run_dir / CONTEXT_JSON
    if not legacy.exists():
        if current.exists() or run_id is None:
            return json.loads(current.read_text())
        ts = _run_start_ts(run_id)
        return {"commit_hash": None, "uv_freeze": [], "start_timestamp": ts}
    try:
        import yaml
    except ImportError:
        raise ImportError("legacy contexts need pyyaml: install aswan[legacy]")
    data = yaml.safe_load(legacy.read_text())
    return {_RENAMED_KEYS.get(k, k): v for k, v in data.items()}


def _zipped_events(path: Path) -> Iterator[tuple[str, bytes]]:
    with zipfile.ZipFile(path) as zf:
        for info in zf.infolist():
            yield info.filename, zf.read(info)


def _loose_events(root: Path, names: list[str]) -> Iterator[tuple[str, bytes]]:
    for name in names:
        yield name, (root / name).read_bytes()


def _read_legacy(path: Path) -> bytes | None:
    try:
        with zipfile.ZipFile(path) as zf:
            return zf.read(_CONTENT)
    except (FileNotFoundError, zipfile.BadZipFile, KeyError):
        return None
