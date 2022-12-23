from itertools import islice
from time import time

import typer
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
        import pandas as pd
        from dash import Dash, dash_table, dcc, html
        from dash.dependencies import Input, Output

        sheets = ["https://codepen.io/chriddyp/pen/bWLwgP.css", dbc.themes.BOOTSTRAP]
        self.DF = pd.DataFrame
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
        surls = (
            self.DF([*source_urls_grouped], columns=["status", "handler", "count"])
            .assign(status=lambda df: df["status"].replace(STATUS_MAP))
            .pivot_table(columns="status", index="handler", values="count")
            .reset_index()
            .fillna(0)
            .to_dict("records")
        )
        cev_dicts = [pcev.cev.dict() for pcev in islice(pcevs, int(cev_limit))]
        return {"source_url_rate": surls, "coll_events": cev_dicts}

    def update_metrics(self, store_data: dict):
        coll_evs = store_data.get("coll_events", [])
        if not coll_evs:
            return []
        cev_df = self.DF(coll_evs)
        todo_n = (
            self.DF(store_data.get("source_url_rate", []))
            .sum()
            .to_dict()
            .get(STATUS_MAP[Statuses.TODO], 0)
        )
        vc = cev_df["status"].value_counts().to_dict()
        since_last = (time() - cev_df["timestamp"].max()) / 60
        mins_past = (cev_df["timestamp"].max() - cev_df["timestamp"].min()) / 60
        in_hour = sum([vc.get(s, 0) for s in SUCCESS_STATUSES]) * 60 / mins_past
        todo_in_hours = todo_n / (in_hour or 0.1)
        pretty_vc = [
            f'{" ".join(STATUS_MAP[k].split("_")).title()}: {v}' for k, v in vc.items()
        ]
        info_lines = [
            f"last {cev_df.shape[0]} events in {parse_time(mins_past)} of work",
            f"statuses: {', '.join(pretty_vc)}",
            f"{parse_time(since_last)} since last event",
            f"estimate for 1 hour: {in_hour:.2f} - ({24 * in_hour:.2f} / day)",
            f"all todos in {parse_time(todo_in_hours * 60)}",
        ]
        info_span = self.html.Span([*map(self.html.H4, info_lines)])
        full_table = self.html.Table([*map(self.cev_to_tr, coll_evs)])
        return self.html.Div([info_span, full_table])

    def update_status(self, store_data: dict):
        surl_rates = store_data.get("source_url_rate", [])
        df = self.DF(surl_rates)
        return self.DashTable(
            df.to_dict("records"), [{"name": i, "id": i} for i in df.columns]
        )

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


app = typer.Typer()


@app.command()
def monitor(
    project: str,
    root: str = None,
    port: int = 6969,
    interval: int = 30,
    debug: bool = False,
):  # pragma: no cover
    depot = AswanDepot(project, root)
    MonitorApp(depot, interval).app.run_server(port=port, debug=debug)


def parse_time(n: float):
    if n > 60:
        return f"{n / 60:.1f} hours"
    if n > 1.5:
        return f"{n:.1f} minutes"
    return f"{n * 60:.1f} seconds"
