"""Minimal dynamic dash app example.

Click on a button, and draw a new plotly-resampler graph of two noisy sinusoids.
This example uses pattern-matching callbacks to update dynamically constructed graphs.
The plotly-resampler graphs themselves are cached on the server side.

The main difference between this example and 02_minimal_cache.py is that here, we want
to cache using a dcc.Store that is not yet available on the client side. As a result we
split up our logic into two callbacks: (1) the callback used to construct the necessary
components and send them to the client-side, and (2) the callback used to construct the
actual plotly-resampler graph and cache it on the server side. These two callbacks are
chained together using the dcc.Interval component.

"""
from data2 import get_doocs_properties, load_parquet_data
from pathlib import Path
from datetime import datetime

from typing import List
from scipy.signal import spectrogram
import numpy as np
import plotly.graph_objects as go
from dash import MATCH, Input, Output, State, dcc, html, no_update
from dash_extensions.enrich import (
    DashProxy,
    Serverside,
    ServersideOutputTransform,
    Trigger,
    TriggerTransform,
)
from trace_updater import TraceUpdater

from plotly_resampler import FigureResampler
from plotly_resampler.aggregation import MinMaxLTTB

# Data that will be used for the plotly-resampler figures
online_data = {}

doocs_properties = get_doocs_properties(Path("C:/Users/pmahad/Desktop/Project/XFEL.SYNC"))

# --------------------------------------Globals ---------------------------------------
app = DashProxy(__name__, transforms=[ServersideOutputTransform(), TriggerTransform()])

app.layout = html.Div(
    [
        html.Div(children=[
            dcc.Dropdown(doocs_properties, id="property-selecter", multi=True),
            html.Button("load data and plot", id="load-plot"),
            dcc.Checklist(id='coarse-tuning-selector', options=[{'label': 'Include Coarse Tuning',
                                                                 'value': 'include_coarse_tuning'}], value=[]),
        ]),

        html.Div(id="container", children=[]),
        html.Div(id="spectrogram-container"),
        html.Div(dcc.Slider(id='window-size-slider', min=1, max=100, step=1, value=10,
                            marks={i: str(i) for i in range(0, 101, 10)})),
    ]
)


# ------------------------------------ DASH logic -------------------------------------
@app.callback(
    Output("container", "children"),
    Input("load-plot", "n_clicks"),
    State("property-selecter", "value"),
    State("container", "children"),
    State("coarse-tuning-selector", "value"),
    prevent_initial_call=True,
)
def add_graph_div(_, selected_properties, div_children: List[html.Div], coarse_tuning):
    if selected_properties is None:
        return []
    else:
        props = [Path(prop) for prop in selected_properties]
        loaded_data = load_parquet_data(props, datetime(2023, 10, 15, 17, 30), datetime(2023, 11, 15, 17, 30))
        for key, item in loaded_data.items():
            online_data[doocs_properties[str(key)]] = item

        div_children = []
        # make the plots here
        for dat in loaded_data:
            uid = doocs_properties[str(dat)]
            new_child = html.Div(
                children=[
                    # The graph and its needed components to serialize and update efficiently
                    # Note: we also add a dcc.Store component, which will be used to link the
                    #       server side cached FigureResampler object
                    dcc.Graph(id={"type": "dynamic-graph", "index": uid}, figure=go.Figure()),
                    dcc.Loading(dcc.Store(id={"type": "store", "index": uid})),
                    TraceUpdater(id={"type": "dynamic-updater", "index": uid}, gdID=f"{uid}"),
                    # This dcc.Interval components makes sure that the `construct_display_graph`
                    # callback is fired once after these components are added to the session
                    # its front-end
                    dcc.Interval(
                        id={"type": "interval", "index": uid}, max_intervals=1, interval=1
                    ),
                    # Add the spectrogram-graph within the container
                    dcc.Graph(id={"type": "spectrogram-graph", "index": uid}, figure=go.Figure()),
                ],
            )

            div_children.append(new_child)
        return div_children


# This method constructs the FigureResampler graph and caches it on the server side
@app.callback(
    [
        Output({"type": "dynamic-graph", "index": MATCH}, "figure"),
        Output({"type": "spectrogram-graph", "index": MATCH}, "figure"),
        Output({"type": "store", "index": MATCH}, "data")
    ],
    [
        Input("load-plot", "n_clicks"),
        Trigger({"type": "interval", "index": MATCH}, "n_intervals")
    ],
    [
        State("property-selecter", "value"),
        State({"type": "dynamic-graph", "index": MATCH}, "id"),
        State('window-size-slider', 'value'),
        State("coarse-tuning-selector", "value")
    ],
    prevent_initial_call=True,
)
def construct_display_graph(n_clicks, _, selected_properties, analysis, window_size, coarse_tuning) -> FigureResampler:
    print("Coarse Tuning Value:", coarse_tuning)
    file_figures = []
    spec_figures = []
    store_data = []
    if selected_properties is None:
        return [], [], []

    fig = FigureResampler(
        go.Figure(),
        default_n_shown_samples=2_000,
        default_downsampler=MinMaxLTTB(parallel=True),
    )

    data = online_data[analysis['index']]

    sigma = n_clicks * 1e-6
    fig.add_trace(dict(name="new"),
                  hf_x=[datetime.fromtimestamp(timestamp) for timestamp in data["timestamp"]],
                  hf_y=data["data"])
    if 'include_coarse_tuning' in coarse_tuning:
        # Include coarse tuning
        if 'include_coarse_tuning' in coarse_tuning:
            # Include coarse tuning
            fig.add_trace(dict(name="coarse tuning"),
                          hf_x=[datetime.fromtimestamp(timestamp) for timestamp in data["coarse_tuning_timestamp"]],
                          hf_y=data["coarse_tuning_data"])
        fig.update_layout(title=f"<b>{analysis['index']}</b>", title_x=0.5)

    spec_data, freqs, times = get_spectrogram(data["data"], fs=1, window_size=window_size)
    spec_fig = go.Figure(go.Heatmap(z=spec_data, x=times, y=freqs, colorscale='Viridis'))

    file_figures.append(fig)
    spec_figures.append(spec_fig)
    store_data.append(Serverside(fig))

    return file_figures, spec_figures, store_data


def get_spectrogram(data, fs, window_size):
    """Function to calculate spectrogram."""
    f, t, Sxx = spectrogram(data, fs, nperseg=window_size)
    return 10 * np.log10(Sxx), f, t


def sync_zoom(relayoutdata: dict, children: List[html.Div]):
    if relayoutdata is None:
        return no_update
    for child in children:
        graph_id = child.get("props", {}).get("children", [])[0].get("props", {}).get("id")
        if graph_id is not None and "type" in graph_id and graph_id["type"] == "dynamic-graph":
            child_figure = relayoutdata.get("figure", {})
            if child_figure:
                child_figure["layout"]["xaxis"] = relayoutdata.get("xaxis", {})
                child_figure["layout"]["yaxis"] = relayoutdata.get("yaxis", {})
                return child_figure
    return no_update


@app.callback(
    Output({"type": "dynamic-updater", "index": MATCH}, "updateData"),
    Input({"type": "dynamic-graph", "index": MATCH}, "relayoutData"),
    State({"type": "store", "index": MATCH}, "data"),
    prevent_initial_call=True,
    memoize=True,
)
def update_fig(relayoutdata: dict, fig: FigureResampler):
    if fig is not None:
        return fig.construct_update_data(relayoutdata)
    return no_update


# --------------------------------- Running the app ---------------------------------
if __name__ == "__main__":
    app.run_server(debug=True, port=9023)
