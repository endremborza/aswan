import os
import sys
from time import time

import dash_bootstrap_components as dbc
import pandas as pd
from dash import Dash, dash_table, dcc, html
from dash.dependencies import Input, Output
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from .constants import Statuses
from .depot import AswanDepot
from .models import CollEvent, SourceUrl
from .object_store import ObjectStore

external_stylesheets = [
    "https://codepen.io/chriddyp/pen/bWLwgP.css",
    dbc.themes.BOOTSTRAP,
]


def update_status(store_data: dict):
    surl_rates = store_data.get("source_url_rate", [])
    df = pd.DataFrame(surl_rates)
    return dash_table.DataTable(
        df.to_dict("records"), [{"name": i, "id": i} for i in df.columns]
    )


class MonitorApp:
    def __init__(self, depot: AswanDepot, refresh_interval_secs=30, cev_limit=100):
        self.app = Dash(__name__, external_stylesheets=external_stylesheets)
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
        )(update_status)

        @self.app.server.route("/object_store/<file_id>")
        def get_obj(file_id):  # pragma: no cover
            out = ObjectStore(self.depot.object_store_path).read(file_id)
            return out if isinstance(out, dict) else {"list": out}

    def update_store(self, _):

        cevs = self.depot.get_handler_events(
            only_latest=False, only_successful=False, limit=self.cev_limit
        )
        session = sessionmaker(self.depot.current.engine)()
        source_urls_grouped = (
            session.query(SourceUrl.current_status, SourceUrl.handler, func.count())
            .group_by(SourceUrl.current_status, SourceUrl.handler)
            .all()
        )
        session.close()
        surls = (
            pd.DataFrame([*source_urls_grouped], columns=["status", "handler", "count"])
            .pivot_table(columns="status", index="handler", values="count")
            .reset_index()
            .fillna(0)
            .to_dict("records")
        )
        return {"source_url_rate": surls, "coll_events": [*map(CollEvent.dict, cevs)]}

    def update_metrics(self, store_data: dict):
        coll_evs = store_data.get("coll_events", [])
        if not coll_evs:
            return []
        cev_df = pd.DataFrame(coll_evs)
        vc = cev_df["status"].value_counts().to_dict()
        upto_now = (time() - cev_df["timestamp"].min()) / 60
        in_hour = vc.get(Statuses.PROCESSED, 0) * 60 / upto_now
        todo_in_hours = vc.get(Statuses.TODO, 0) / (in_hour or 0.1)
        info_lines = [
            f"last {self.cev_limit} statuses: {vc}",
            f"estimate for 1 hour: {in_hour:.2f} - ({24 * in_hour:.2f} / day)",
            f"all todos in {todo_in_hours:.2f} hours",
        ]
        return html.Div(
            [
                html.H3(f"Results in last {upto_now:.2f} minutes"),
                html.Span([*map(html.P, info_lines)]),
                html.Table([*map(cev_to_tr, coll_evs)]),
            ]
        )


def run_monitor_app(
    depot: AswanDepot, port_no=6969, refresh_interval_secs=30, silent=True, debug=False
):  # pragma: no cover
    if silent:
        sys.stdout = open(os.devnull, "w")
        sys.stderr = open(os.devnull, "w")
    MonitorApp(depot, refresh_interval_secs).app.run_server(port=port_no, debug=debug)


def cev_to_tr(cev_d: dict):
    cev = CollEvent(**cev_d)
    tds = [cev.iso, cev.handler, cev.status, html.A(cev.url, href=cev.url)]
    link = (
        html.A("file", href=f"/object_store/{cev.output_file}")
        if cev.output_file
        else None
    )
    return html.Tr([*map(html.Td, [*tds, link])])
