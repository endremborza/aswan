import os
import sys
from itertools import islice
from pathlib import Path
from time import time

from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from .constants import Statuses
from .depot import AswanDepot
from .models import CollEvent, SourceUrl
from .object_store import ObjectStore


class MonitorApp:
    def __init__(self, depot: AswanDepot, refresh_interval_secs=30, cev_limit=100):
        import dash_bootstrap_components as dbc
        import pandas as pd
        from dash import Dash, dash_table, dcc, html
        from dash.dependencies import Input, Output

        self.DF = pd.DataFrame
        self.DashTable = dash_table.DataTable
        self.html = html
        self.app = Dash(
            __name__,
            external_stylesheets=[
                "https://codepen.io/chriddyp/pen/bWLwgP.css",
                dbc.themes.BOOTSTRAP,
            ],
        )
        self.depot = depot
        self.cev_limit = cev_limit
        elems = [
            html.H4("Collection monitor"),
            dcc.Store(id="data-store", storage_type="memory"),
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
            [Input("interval-component", "n_intervals")],
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

    def update_store(self, _):

        # TODO: possibly read past runs
        pcevs = self.depot.get_handler_events(
            only_latest=False, only_successful=False, from_current=True
        )
        session = sessionmaker(self.depot.current.engine)()
        source_urls_grouped = (
            session.query(SourceUrl.current_status, SourceUrl.handler, func.count())
            .group_by(SourceUrl.current_status, SourceUrl.handler)
            .all()
        )
        session.close()
        surls = (
            self.DF([*source_urls_grouped], columns=["status", "handler", "count"])
            .pivot_table(columns="status", index="handler", values="count")
            .reset_index()
            .fillna(0)
            .to_dict("records")
        )
        cev_dicts = [pcev.cev.dict() for pcev in islice(pcevs, self.cev_limit)]
        return {"source_url_rate": surls, "coll_events": cev_dicts}

    def update_metrics(self, store_data: dict):
        coll_evs = store_data.get("coll_events", [])
        if not coll_evs:
            return []
        cev_df = self.DF(coll_evs)
        vc = cev_df["status"].value_counts().to_dict()
        upto_now = (time() - cev_df["timestamp"].min()) / 60
        in_hour = vc.get(Statuses.PROCESSED, 0) * 60 / upto_now
        todo_in_hours = vc.get(Statuses.TODO, 0) / (in_hour or 0.1)
        info_lines = [
            f"last {self.cev_limit} statuses: {vc}",
            f"estimate for 1 hour: {in_hour:.2f} - ({24 * in_hour:.2f} / day)",
            f"all todos in {todo_in_hours:.2f} hours",
        ]
        return self.html.Div(
            [
                self.html.H3(f"Results in last {upto_now:.2f} minutes"),
                self.html.Span([*map(self.html.P, info_lines)]),
                self.html.Table([*map(self.cev_to_tr, coll_evs)]),
            ]
        )

    def update_status(self, store_data: dict):
        surl_rates = store_data.get("source_url_rate", [])
        df = self.DF(surl_rates)
        return self.DashTable(
            df.to_dict("records"), [{"name": i, "id": i} for i in df.columns]
        )

    def cev_to_tr(self, cev_d: dict):
        cev = CollEvent(**cev_d)
        tds = [cev.iso, cev.handler, cev.status, self.html.A(cev.url, href=cev.url)]
        link = (
            self.html.A("file", href=f"/object_store/{cev.output_file}")
            if cev.output_file
            else None
        )
        return self.html.Tr([*map(self.html.Td, [*tds, link])])


def run_monitor_app(
    depot_root: Path, port_no=6969, refresh_interval_secs=30, silent=True, debug=False
):  # pragma: no cover
    if silent:
        sys.stdout = open(os.devnull, "w")
        sys.stderr = open(os.devnull, "w")
    MonitorApp(
        AswanDepot(depot_root.name, depot_root.parent), refresh_interval_secs
    ).app.run_server(port=port_no, debug=debug)
