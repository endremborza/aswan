from collections import Counter
from itertools import islice
from time import time

from sqlalchemy.orm import sessionmaker

from .constants import SUCCESS_STATUSES, Statuses
from .depot import AswanDepot
from .metadata_handling import get_grouped_surls
from .models import CollEvent
from .object_store import ObjectStore

STATUS_MAP = {v: k for k, v in Statuses.__dict__.items() if not k.startswith("_")}


class MonitorApp:
    def __init__(self, depot: AswanDepot, refresh_interval_secs=30):
        import dash_bootstrap_components as dbc
        from dash import Dash, dash_table, dcc, html
        from dash.dependencies import Input, Output

        sheets = ["https://codepen.io/chriddyp/pen/bWLwgP.css", dbc.themes.BOOTSTRAP]
        self.DashTable = dash_table.DataTable
        self.html = html
        self.app = Dash(__name__, external_stylesheets=sheets)
        self.depot = depot
        self.depot.current.setup()
        self._Session = sessionmaker(self.depot.current.engine)
        elems = [
            html.H2("Collection monitor"),
            dcc.Store(id="data-store", storage_type="memory"),
            dcc.Dropdown(
                [
                    {"value": i, "label": f"show last {i} events"}
                    for i in [50, 100, 500, 1000, 5000]
                ],
                value=100,
                id="cev-count",
                style={"margin": "15px"},
            ),
            html.Div(id="live-update-status"),
            html.Div(id="live-update-text", style={"padding": 30}),
            dcc.Interval(
                id="interval-component",
                interval=refresh_interval_secs * 1000,
                n_intervals=0,
            ),
        ]
        self.app.layout = html.Div(elems, style={"padding": 15})

        self.app.callback(
            Output("data-store", "data"),
            [Input("interval-component", "n_intervals"), Input("cev-count", "value")],
        )(self.update_store)

        self.app.callback(
            Output("live-update-text", "children"),
            [Input("data-store", "data")],
        )(self.update_metrics)

        self.app.callback(
            Output("live-update-status", "children"),
            [Input("data-store", "data")],
        )(self.update_status)

        @self.app.server.route("/object_store/<file_id>")
        def get_obj(file_id):  # pragma: no cover
            out = ObjectStore(self.depot.object_store_path).read(file_id)
            return out if isinstance(out, dict) else {"list": out}

    def update_store(self, _, cev_limit):
        # TODO: possibly read past runs
        pcevs = self.depot.get_handler_events(
            only_latest=False, only_successful=False, from_current=True
        )
        session = self._Session()
        source_urls_grouped = get_grouped_surls(session)
        session.close()
        cev_dicts = [pcev.cev.dict() for pcev in islice(pcevs, int(cev_limit))]
        return {
            "source_url_rate": _status_pivot(source_urls_grouped),
            "coll_events": cev_dicts,
        }

    def update_metrics(self, store_data: dict):
        coll_evs = store_data.get("coll_events", [])
        if not coll_evs:
            return []
        todo = STATUS_MAP[Statuses.TODO]
        todo_n = sum(row.get(todo, 0) for row in store_data.get("source_url_rate", []))
        vc = Counter(cev["status"] for cev in coll_evs).most_common()
        stamps = [cev["timestamp"] for cev in coll_evs]
        since_last = (time() - max(stamps)) / 60
        mins_past = (max(stamps) - min(stamps)) / 60
        n_success = sum(n for status, n in vc if status in SUCCESS_STATUSES)
        in_hour = n_success * 60 / mins_past if mins_past else 0.0
        todo_in_hours = todo_n / (in_hour or 0.1)
        pretty_vc = [
            f"{' '.join(STATUS_MAP[k].split('_')).title()}: {v}" for k, v in vc
        ]
        info_lines = [
            f"last {len(coll_evs)} events in {parse_time(mins_past)} of work",
            f"statuses: {', '.join(pretty_vc)}",
            f"{parse_time(since_last)} since last event",
            f"estimate for 1 hour: {in_hour:.2f} - ({24 * in_hour:.2f} / day)",
            f"all todos in {parse_time(todo_in_hours * 60)}",
        ]
        info_span = self.html.Span([*map(self.html.H4, info_lines)])
        full_table = self.html.Table([*map(self.cev_to_tr, coll_evs)])
        return self.html.Div([info_span, full_table])

    def update_status(self, store_data: dict):
        rows = store_data.get("source_url_rate", [])
        columns = [{"name": k, "id": k} for k in (rows[0] if rows else [])]
        return self.DashTable(rows, columns)

    def cev_to_tr(self, cev_d: dict):
        cev = CollEvent(**cev_d)
        tds = [
            cev.iso,
            cev.handler,
            STATUS_MAP[cev.status],
            self.html.A(cev.url, href=cev.url),
        ]
        link = (
            self.html.A("file", href=f"/object_store/{cev.output_file}")
            if cev.output_file
            else None
        )
        return self.html.Tr([*map(self.html.Td, [*tds, link])])


def serve(depot: AswanDepot, port: int, interval: int, debug: bool) -> None:
    try:
        import dash  # noqa: F401
    except ImportError:
        raise SystemExit("Install aswan[monitor] to use the dashboard.")
    MonitorApp(depot, interval).app.run(port=port, debug=debug)


def _status_pivot(grouped: list[tuple[str, str, int]]) -> list[dict]:
    """One row per handler, a count column per status present (absent → 0)."""
    statuses = sorted({STATUS_MAP[status] for status, _, _ in grouped})
    rows: dict[str, dict] = {}
    for status, handler, count in grouped:
        row = rows.setdefault(
            handler, {"handler": handler, **dict.fromkeys(statuses, 0)}
        )
        row[STATUS_MAP[status]] = count
    return [rows[h] for h in sorted(rows)]


def parse_time(n: float):
    if n > 60:
        return f"{n / 60:.1f} hours"
    if n > 1.5:
        return f"{n:.1f} minutes"
    return f"{n * 60:.1f} seconds"
