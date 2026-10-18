"""The `aswan` command."""

import argparse
import logging
from pathlib import Path

from .depot import AswanDepot


def main() -> None:
    parser = argparse.ArgumentParser(prog="aswan")
    sub = parser.add_subparsers(dest="command", required=True)
    mon = sub.add_parser("monitor", help="serve the collection dashboard")
    mon.add_argument("project")
    mon.add_argument("--root", default=None)
    mon.add_argument("--port", type=int, default=6969)
    mon.add_argument("--interval", type=int, default=30)
    mon.add_argument("--debug", action="store_true")
    mig = sub.add_parser(
        "migrate",
        help="copy the runs of a pre-1.0 depot into a current one",
        description="Copy the runs of the pre-1.0 depot LEGACY that the depot"
        " TARGET lacks, re-storing their objects; created if missing.",
    )
    mig.add_argument("legacy", type=Path)
    mig.add_argument("target", type=Path)
    args = parser.parse_args()
    if args.command == "monitor":
        from .monitor_app import serve

        serve(AswanDepot(args.project, args.root), args.port, args.interval, args.debug)
    else:
        from .legacy import migrate_depot

        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
        target = args.target.absolute()
        report = migrate_depot(args.legacy, AswanDepot(target.name, target.parent))
        print(report)
        if report.broken:
            raise SystemExit(1)
