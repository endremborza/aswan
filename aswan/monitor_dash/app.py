import datetime as dt
import time
from typing import Dict

import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objects as go
from dash.dependencies import Input, Output
from sqlalchemy import func
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from ..constants import Envs, Statuses
from ..models import CollectionEvent, SourceUrl
from ..object_store import ObjectStoreBase

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
    _proc_in_1_hour = round(
        _vc.get(Statuses.PROCESSED, 0) * 60 / LAST_N_MINS, 2
    )
    info_span = [
        html.P(f"statuses in last {LAST_N_MINS} minutes: {_vc}"),
        html.P(
            f"projection for 1 hour: {_proc_in_1_hour} -"
            f" ({24 * _proc_in_1_hour} / day)"
        ),
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


def update_graph_live(store_data):
    surl_rates = store_data.get("source_url_rate", {})

    fig = go.Figure()
    for handler, statuses in surl_rates.items():
        fig.add_trace(
            go.Bar(
                x=list(statuses.keys()),
                y=list(statuses.values()),
                name=handler,
            )
        )
    fig.update_layout(
        barmode="stack", xaxis={"categoryorder": "category ascending"}
    )
    return fig


class MonitorApp:
    def __init__(
        self,
        engine_dict: Dict[str, Engine],
        object_stores: Dict[str, ObjectStoreBase],
        refresh_interval_secs=30,
    ):

        self.app = dash.Dash(
            __name__, external_stylesheets=external_stylesheets
        )
        self.app.layout = html.Div(
            [
                dcc.Tabs(
                    id="env-tabs",
                    value=Envs.PROD,
                    children=[
                        dcc.Tab(label=env, value=env)
                        for env in engine_dict.keys()
                    ],
                ),
                html.Div(
                    [
                        html.H4("Collection monitor"),
                        dcc.Store(id="data-store", storage_type="memory"),
                        dcc.Graph(id="live-update-graph"),
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
        self._sessions = {
            k: sessionmaker(engine) for k, engine in engine_dict.items()
        }
        self.object_stores = object_stores
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
            Output("live-update-graph", "figure"),
            [Input("data-store", "data")],
        )(update_graph_live)

        @self.app.server.route("/object_store/<env_id>/<file_id>")
        def get_obj(env_id, file_id):
            out = self.object_stores[env_id].read_json(file_id)
            if isinstance(out, dict):
                return out
            else:
                return {"list": out}

    def update_store(self, _, env_id):
        session = self._sessions[env_id]()
        coll_events = (
            session.query(CollectionEvent)
            .filter(CollectionEvent.timestamp > time.time() - LAST_N_MINS * 60)
            .all()
        )
        source_urls_grouped = (
            session.query(
                SourceUrl.current_status, SourceUrl.handler, func.count()
            )
            .group_by(SourceUrl.current_status, SourceUrl.handler)
            .all()
        )
        session.close()

        url_rates = {}
        for _status, _handler, _count in source_urls_grouped:
            url_rates[_handler] = {
                _status: _count,
                **url_rates.get(_handler, {}),
            }
        return {
            "source_url_rate": url_rates,
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


def get_monitor_app(
    engine_dict: Dict[str, Engine],
    object_stores: Dict[str, ObjectStoreBase],
    refresh_interval_secs=30,
):

    return MonitorApp(engine_dict, object_stores, refresh_interval_secs).app
