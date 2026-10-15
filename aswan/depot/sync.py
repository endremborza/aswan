"""Rsync-based depot synchronisation.

Runs are immutable and the status files are a cache that `get_complete_status`
repairs by replaying missing runs, so both transfer with `--ignore-existing`.
The object store's ark is rewritten on every commit, so it syncs through
dirark's index-first merge instead.
"""

import os
import subprocess
from typing import TYPE_CHECKING

from ..constants import REMOTE_ENV_VAR

if TYPE_CHECKING:
    from .base import AswanDepot


def push(depot: "AswanDepot", remote: str | None) -> None:
    target = _remote_depot(depot, remote)
    _rsync(depot, f"{depot.root}/", f"{target}/")
    depot.object_store.push(f"{target}/{depot.object_store_path.name}")


def pull(depot: "AswanDepot", remote: str | None) -> None:
    target = _remote_depot(depot, remote)
    if not _exists(target):
        return
    _rsync(depot, f"{target}/", f"{depot.root}/")
    depot.object_store.pull(f"{target}/{depot.object_store_path.name}")


def _rsync(depot: "AswanDepot", src: str, dst: str) -> None:
    local_only = [depot.current.root, depot.object_store_path]
    excludes = [f"--exclude=/{p.name}/" for p in local_only]
    subprocess.run(
        ["rsync", "-az", "--ignore-existing", *excludes, src, dst], check=True
    )


def _exists(target: str) -> bool:
    listing = subprocess.run(
        ["rsync", "--list-only", "--ignore-missing-args", target],
        check=True,
        capture_output=True,
        text=True,
    )
    return bool(listing.stdout.strip())


def _remote_depot(depot: "AswanDepot", remote: str | None) -> str:
    root = remote or os.environ.get(REMOTE_ENV_VAR)
    if not root:
        raise ValueError(f"No remote specified and {REMOTE_ENV_VAR} env var not set")
    return f"{root}/{depot.name}"
