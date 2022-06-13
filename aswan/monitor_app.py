import datetime as dt
import os
import sys
import time

import dash_bootstrap_components as dbc
import pandas as pd
from dash import Dash, dash_table, dcc, html
from dash.dependencies import Input, Output
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from .config_class import AswanConfig
from .constants import Statuses
from .models import CollectionEvent, SourceUrl

external_stylesheets = [
    "https://codepen.io/chriddyp/pen/bWLwgP.css",
    dbc.themes.BOOTSTRAP,
]

LAST_N_MINS = 5


def update_metrics(store_data):
    coll_evs = store_data.get("coll_events", [])
    if coll_evs:
        _vc = pd.DataFrame(coll_evs)["status"].value_counts().to_dict()
    else:
        _vc = {}
    _proc_in_1_hour = round(_vc.get(Statuses.PROCESSED, 0) * 60 / LAST_N_MINS, 2)
    _todo_in_hours = round(_vc.get(Statuses.TODO, 0) / (_proc_in_1_hour or 0.1), 2)
    info_span = [
        html.P(f"statuses in last {LAST_N_MINS} minutes: {_vc}"),
        html.P(
            f"projection for 1 hour: {_proc_in_1_hour} -"
            f" ({24 * _proc_in_1_hour} / day)"
        ),
        html.P(f"all todos in {_todo_in_hours} hours"),
    ]
    trs = []
    for cev in coll_evs:
        trs.append(
            html.Tr(
                [
                    html.Td(e)
                    for e in [
                        cev["datetime"],
                        cev["handler"],
                        cev["status"],
                        html.A(cev["url"], href=cev["url"]),
                        html.A(
                            "file",
                            href=f"/object_store/{cev['env']}/{cev['file']}",
                        )
                        if cev["file"]
                        else None,
                    ]
                ]
            )
        )
    return html.Div(
        [
            html.H3(f"Results in last {LAST_N_MINS} minutes"),
            html.Span(info_span),
            html.Table(trs),
        ]
    )


def update_status(store_data):
    surl_rates = store_data.get("source_url_rate", [])
    df = pd.DataFrame(surl_rates)
    return dash_table.DataTable(
        df.to_dict("records"), [{"name": i, "id": i} for i in df.columns]
    )


class MonitorApp:
    def __init__(
        self,
        conf: AswanConfig,
        refresh_interval_secs=30,
    ):
        self.conf = conf
        self.app = Dash(__name__, external_stylesheets=external_stylesheets)
        self.app.layout = html.Div(
            [
                dcc.Tabs(
                    id="env-tabs",
                    value=AswanConfig.prod_name,
                    children=[
                        dcc.Tab(label=e, value=e) for e in self.conf.env_dict.keys()
                    ],
                ),
                html.Div(
                    [
                        html.H4("Collection monitor"),
                        dcc.Store(id="data-store", storage_type="memory"),
                        html.Div(id="live-update-status"),
                        html.Div(id="live-update-text", style={"padding": 30}),
                        dcc.Interval(
                            id="interval-component",
                            interval=refresh_interval_secs * 1000,
                            n_intervals=0,
                        ),
                    ],
                    style={"padding": 15},
                ),
            ],
            style={"padding": 20},
        )
        self._add_callbacks()

    def _add_callbacks(self):
        self.app.callback(
            Output("data-store", "data"),
            [
                Input("interval-component", "n_intervals"),
                Input("env-tabs", "value"),
            ],
        )(self.update_store)

        self.app.callback(
            Output("live-update-text", "children"),
            [Input("data-store", "data")],
        )(update_metrics)

        self.app.callback(
            Output("live-update-status", "children"),
            [Input("data-store", "data")],
        )(update_status)

        @self.app.server.route("/object_store/<env_id>/<file_id>")
        def get_obj(env_id, file_id):
            out = self.conf.env_dict[env_id].object_store.read_json(file_id)
            if isinstance(out, dict):
                return out
            else:
                return {"list": out}

    def update_store(self, _, env_id):
        session = sessionmaker(self.conf.env_dict[env_id].get_engine())()
        coll_events = (
            session.query(CollectionEvent)
            .filter(CollectionEvent.timestamp > time.time() - LAST_N_MINS * 60)
            .all()
        )
        source_urls_grouped = (
            session.query(SourceUrl.current_status, SourceUrl.handler, func.count())
            .group_by(SourceUrl.current_status, SourceUrl.handler)
            .all()
        )
        session.close()

        _df = (
            pd.DataFrame([*source_urls_grouped], columns=["status", "handler", "count"])
            .pivot_table(columns="status", index="handler", values="count")
            .reset_index()
            .fillna(0)
        )
        return {
            "source_url_rate": _df.to_dict("records"),
            "coll_events": [
                {
                    "status": ce.status,
                    "env": env_id,
                    "handler": ce.handler,
                    "url": ce.url,
                    "file": ce.output_file,
                    "datetime": dt.datetime.fromtimestamp(ce.timestamp),
                }
                for ce in coll_events
                if ce is not None
            ],
        }


def run_monitor_app(
    conf, port_no=6969, refresh_interval_secs=30, silent=True, debug=False
):
    if silent:
        sys.stdout = open(os.devnull, "w")
        sys.stderr = open(os.devnull, "w")
    MonitorApp(conf, refresh_interval_secs).app.run_server(port=port_no, debug=debug)
