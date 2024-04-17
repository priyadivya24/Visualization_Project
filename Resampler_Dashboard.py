from data_arne import get_doocs_properties, load_parquet_data
from pathlib import Path
from datetime import datetime, timedelta

from typing import List
import pandas as pd
import plotly.graph_objects as go
from dash import MATCH, Input, Output, State, dcc, html, no_update, ClientsideFunction
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
from os.path import relpath
import numpy as np
from scipy import signal
from plotly.subplots import make_subplots

# Data that will be used for the plotly-resampler figures
online_data = {}

doocs_properties = get_doocs_properties(Path("C:/Users/pmahad/PycharmProjects/pythonProject/Database"))
app = DashProxy(__name__, transforms=[ServersideOutputTransform(), TriggerTransform()])

# app layout
app.layout = html.Div([
    dcc.Tabs(id="tabs", value='XTIN', children=[
        dcc.Tab(label='XTIN', value='XTIN'),
        dcc.Tab(label='XHEXP1', value='XHEXP1'),
    ]),
    html.Div(id="tabs-content", children=[]),
    dcc.Dropdown(id="property-selecter", value='', placeholder="Select LASER File(s)", multi=True),
    dcc.Dropdown(id="property-selecter1", value='', placeholder="Select LSU File(s)", multi=True),
    dcc.Dropdown(id="property-selecter2", value='',  placeholder="Select CLIMATE File(s)", multi=True),
    dcc.DatePickerRange(
        id='date-picker',
        min_date_allowed=datetime(2023, 10, 15),
        max_date_allowed=datetime(2023, 11, 15),
        initial_visible_month=datetime(2023, 10, 15),
        start_date=datetime(2023, 10, 15),
        end_date=datetime(2023, 11, 15),
        minimum_nights=0
    ),
    dcc.RangeSlider(
        id='time-range',
        min=0,
        max=23,
        step=0.1,
        value=[0, 23],
        marks={i: {'label': str(i)} for i in range(0, 25)},
    ),
    html.Button("Load Data and Plot", id="load-plot"),
    html.Div(id="container", children=[]),
])

@app.callback(
    Output('property-selecter', 'options'),
    Output('property-selecter1', 'options'),
    Output('property-selecter2', 'options'),
    [Input('tabs', 'value')]
)
def update_dropdown_options(tab):
    if tab == 'XTIN':
        properties = [prop for prop in doocs_properties.keys() if 'XTIN' in prop and 'LASER.LOCK.XLO' in prop]
        properties1 = [prop for prop in doocs_properties.keys() if 'XTIN' in prop and 'LINK.LOCK' in prop]
        properties2 = [prop for prop in doocs_properties.keys() if 'XTIN' in prop and 'CLIMATE' in prop]

    elif tab == 'XHEXP1':
        properties = [prop for prop in doocs_properties.keys() if 'XHEXP1' in prop and 'LASER.LOCK.XLO' in prop]
        properties1 = [prop for prop in doocs_properties.keys() if 'XHEXP1' in prop and 'LINK.LOCK' in prop]
        properties2 = [prop for prop in doocs_properties.keys() if 'XHEXP1' in prop and 'CLIMATE' in prop]
    else:
        properties = []
        properties1 = []
        properties2 = []

    return generate_dropdown_options(properties, properties1,properties2)


def generate_dropdown_options(properties=None, properties1=None, properties2=None):
    if properties and properties1 and properties2 is None:
        return []
    dropdown_options = [{'label': 'XFEL.SYNC' + prop.split('XFEL.SYNC')[1], 'value': prop} for prop in properties]
    dropdown_options1 = [{'label': 'XFEL.SYNC' + prop.split('XFEL.SYNC')[1], 'value': prop} for prop in properties1]
    dropdown_options2 = [{'label': 'XFEL.SYNC' + prop.split('XFEL.SYNC')[1], 'value': prop} for prop in properties2]
    return dropdown_options,dropdown_options1, dropdown_options2


@app.callback(
    Output("container", "children"),
    Input("load-plot", "n_clicks"),
    State("property-selecter", "value"),
    State("property-selecter1", "value"),
    State("property-selecter2", "value"),
    State("date-picker", "start_date"),
    State("date-picker", "end_date"),
    State("time-range", "value"),
    prevent_initial_call=True,
)
def add_graph_div(n_clicks, laser_files, link_files, climate_files, start_date, end_date, time_range):
    selected_properties = []
    selected_properties.extend(laser_files if laser_files else [])
    selected_properties.extend(link_files if link_files else [])
    selected_properties.extend(climate_files if climate_files else [])

    if not selected_properties:  # Check if no properties are selected
        return []

    props = [Path(prop) for prop in selected_properties]
    start_dt = datetime.strptime(start_date.split('T')[0], "%Y-%m-%d")
    end_dt = datetime.strptime(end_date.split('T')[0], "%Y-%m-%d")
    start_time = time_range[0]
    end_time = time_range[1]

    start_dt = start_dt.replace(hour=int(start_time), minute=int((start_time - int(start_time)) * 60))

    end_dt = end_dt.replace(hour=int(end_time), minute=int((end_time - int(end_time)) * 60))


    loaded_data = load_parquet_data(props, start_dt, end_dt)

    # Clear previous graph divs
    div_children = []

    for key, item in loaded_data.items():
        uid = doocs_properties.get(str(key), f"graph-{len(div_children)}")  # Generate a unique ID
        online_data[uid] = item  # Assuming `online_data` and `doocs_properties` are defined elsewhere

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
        div_children.append(new_child)

    return div_children


@app.callback(
    Output("container", "children", allow_duplicate=True),
    Input("tabs", "value"),
    prevent_initial_call=True,
)
def update_container(tab):
    if tab != tab:
        return []

@app.callback(
    Output({"type": "dynamic-graph", "index": MATCH}, "figure"),
    Output({"type": "store", "index": MATCH}, "data"),
    State("load-plot", "n_clicks"),
    State({"type": "dynamic-graph", "index": MATCH}, "id"),
    Trigger({"type": "interval", "index": MATCH}, "n_intervals"),
    prevent_initial_call=True,
)
def construct_display_graph(n_clicks, analysis) -> FigureResampler:
    fig = FigureResampler(make_subplots(
        rows=2, cols=2, horizontal_spacing=0.03,
        row_heights=[5.0, 0.4],
        #shared_xaxes=True,
        #shared_yaxes=True
    ),
        # create_overview=True,
        # overview_row_idxs=[1, 0],
        default_n_shown_samples=2_000,
        default_downsampler=MinMaxLTTB(parallel=True),
    )
    data = online_data[analysis['index']]
    print(analysis)
    timestamps = pd.to_datetime(data["timestamp"], unit='s')
    sigma = n_clicks * 1e-6

    # First graph (line plot)
    fig.add_trace(go.Scatter(x=timestamps, y=data["data"], name="new", legend='legend1'), row=1, col=1)

    # Second graph (spectrogram)
    data_array = np.array(data["data"].to_numpy())
    spec_data, freqs, times = get_spectrogram(data_array, fs=10)
    fig.add_trace(go.Heatmap(z=spec_data, x=times, y=freqs, colorscale='Viridis', name="spect", legend='legend2'),
                  row=1, col=2)

    fig.update_layout(title=f"<b>{analysis['index']}</b>", title_x=0.5)

    return fig, Serverside(fig)


@app.callback(
    Output({"type": "spectrogram", "index": MATCH}, "data"),
    Input({"type": "dynamic-graph", "index": MATCH}, "data"),
    prevent_initial_call=True,
)
def get_spectrogram(data, fs):
    """Function to calculate spectrogram."""
    f, t, Sxx = signal.spectrogram(data, fs, nperseg=256)
    return 10 * np.log10(Sxx), f, t


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

# Running the app
if __name__ == "__main__":
    app.run_server(debug=True, port=9023)
