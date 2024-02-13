from typing import List
import os
from uuid import uuid4
import pandas as pd
import glob
import plotly.graph_objects as go
from dash import MATCH, Input, Output, State, dcc, html, no_update
from dash_extensions.enrich import (
    DashProxy,
    ServersideOutputTransform,
    Trigger,
    TriggerTransform,
)
from trace_updater import TraceUpdater
from plotly_resampler.aggregation import MinMaxLTTB
from plotly_resampler import FigureResampler

#  path
main_folder = "XFEL.SYNC"
ml01_folder = 'D:/mahad/PyCharm Community Edition 2023.3.2/XFEL/XFEL.SYNC/LASER.LOCK.XLO/XTIN.MLO1'
sl01_folder = 'D:/mahad/PyCharm Community Edition 2023.3.2/XFEL/XFEL.SYNC/LASER.LOCK.XLO/XHEXP1.SLO1'


# Read the parquet files
def read_parquet_files(folder_path):
    return pd.concat([pd.read_parquet(file) for file in glob.glob(folder_path + '/*.parquet')])


ml01_subfolders = [f.name for f in os.scandir(ml01_folder) if f.is_dir()]
sl01_subfolders = [f.name for f in os.scandir(sl01_folder) if f.is_dir()]

# --------------------------------------Globals ---------------------------------------
app = DashProxy(__name__, transforms=[ServersideOutputTransform(), TriggerTransform()])

app.layout = html.Div(
    [
        dcc.Dropdown(
            id='ml01-subfolder-dropdown',
            options=[{'label': subfolder, 'value': subfolder} for subfolder in ml01_subfolders],
            value=ml01_subfolders[0] if ml01_subfolders else None
        ),
        dcc.Dropdown(
            id='sl01-subfolder-dropdown',
            options=[{'label': subfolder, 'value': subfolder} for subfolder in sl01_subfolders],
            value=sl01_subfolders[0] if sl01_subfolders else None
        ),
        html.Button("Add Chart", id="add-chart", n_clicks=0),
        html.Div(id="container", children=[]),
    ]
)


# ------------------------------------ DASH logic -------------------------------------
@app.callback(
    Output("container", "children"),
    Input("add-chart", "n_clicks"),
    State("container", "children"),
    prevent_initial_call=True,
)
def add_graph_div(n_clicks: int, div_children: List[html.Div]):
    uid = str(uuid4())
    new_child = html.Div(
        children=[
            dcc.Graph(id={"type": "dynamic-graph", "index": uid}, figure=go.Figure()),
            dcc.Loading(dcc.Store(id={"type": "store", "index": uid})),
            TraceUpdater(id={"type": "dynamic-updater", "index": uid}, gdID=f"{uid}"),
            dcc.Interval(
                id={"type": "interval", "index": uid}, max_intervals=1, interval=1
            ),
        ],
    )
    return new_child


@app.callback(
    Output({"type": "dynamic-graph", "index": MATCH}, "figure"),
    Output({"type": "store", "index": MATCH}, "data"),
    State("add-chart", "n_clicks"),
    Trigger({"type": "interval", "index": MATCH}, "n_intervals"),
    State('ml01-subfolder-dropdown', 'value'),
    State('sl01-subfolder-dropdown', 'value'),
    prevent_initial_call=True,
)
def construct_display_graph(n_clicks, ml01_subfolder, sl01_subfolder) -> (go.Figure, go.Figure):
    # Define the subfolder paths
    ml01_subfolder_path = os.path.join(ml01_folder, ml01_subfolder)
    sl01_subfolder_path = os.path.join(sl01_folder, sl01_subfolder)

    ml01_data = read_parquet_files(ml01_subfolder_path)
    sl01_data = read_parquet_files(sl01_subfolder_path)
    ml01_data['timestamp'] = pd.to_datetime(ml01_data['timestamp'], unit='s')
    sl01_data['timestamp'] = pd.to_datetime(sl01_data['timestamp'], unit='s')

    # Create FigureResampler objects with MinMaxLTTB aggregation
    ml01_fig_resampler = FigureResampler(
        go.Figure(),
        default_n_shown_samples=2_000,
        default_downsampler=MinMaxLTTB(),
    )
    sl01_fig_resampler = FigureResampler(
        go.Figure(),
        default_n_shown_samples=2_000,
        default_downsampler=MinMaxLTTB()
    )

    # Add traces to the FigureResampler objects
    ml01_fig_resampler.add_trace(
        dict(name="XTIN.MLO1", hf_x=ml01_data['data'].values, hf_y=ml01_data['timestamp'].values))
    sl01_fig_resampler.add_trace(
        dict(name="XHEXP1.SLO1", hf_x=sl01_data['data'].values, hf_y=sl01_data['timestamp'].values))

    # Construct figures from FigureResampler objects
    ml01_fig = ml01_fig_resampler.construct_fig()
    sl01_fig = sl01_fig_resampler.construct_fig()

    ml01_fig.update_layout(title=f"<b>graph - {n_clicks}</b>", title_x=0.5)
    sl01_fig.update_layout(title=f"<b>graph - {n_clicks}</b>", title_x=0.5)

    return ml01_fig, sl01_fig


@app.callback(
    Output({"type": "dynamic-updater", "index": MATCH}, "updateData"),
    Input({"type": "dynamic-graph", "index": MATCH}, "relayoutData"),
    State({"type": "store", "index": MATCH}, "data"),
    prevent_initial_call=True,
    memoize=True,
)
def update_fig(relayoutdata: dict, stored_data):
    if stored_data is not None:
        fig_data = stored_data.get('fig_data')
        if fig_data is not None:
            fig_data['layout']['relayoutData'] = relayoutdata
            return [fig_data]  # Return as array
    return []  # Return an empty array if there's no update



if __name__ == "__main__":
    app.run_server(debug=True, port=9023)
