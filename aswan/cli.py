"""The `aswan` command."""

import logging
from pathlib import Path
from typing import Annotated

from protocli import FILES, Complete, Dispatcher

from .depot import AswanDepot, depot_root


def _depot_names() -> list[str]:
    root = depot_root()
    return sorted(p.name for p in root.iterdir() if p.is_dir()) if root.is_dir() else []


def _monitor(
    project: Annotated[str, Complete(_depot_names)],
    *,
    root: Annotated[str | None, FILES] = None,
    port: int = 6969,
    interval: int = 30,
    debug: bool = False,
) -> None:
    """Serve the collection dashboard.

    Args:
        project: depot name
        root: directory holding the depot, ASWAN_DEPOT_ROOT or the default
            when omitted
        port: port to serve on
        interval: refresh interval in seconds
        debug: run the server in debug mode
    """
    from .monitor_app import serve

    serve(AswanDepot(project, root), port, interval, debug)


def _migrate(
    legacy: Annotated[Path, FILES], target: Annotated[Path, FILES]
) -> int | None:
    """Copy the runs of a pre-1.0 depot into a current one.

    Args:
        legacy: pre-1.0 depot directory
        target: current depot directory, created if missing; only the runs it
            lacks are copied, their objects re-stored
    """
    from .legacy import migrate_depot

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    target = target.absolute()
    report = migrate_depot(legacy, AswanDepot(target.name, target.parent))
    print(report)
    return 1 if report.broken else None


main = Dispatcher("aswan", {"monitor": _monitor, "migrate": _migrate}).run
