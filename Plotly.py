import os
from pathlib import Path
import time
from typing import List
import dash
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from dash import dcc, html, callback_context, no_update, Output, Input, State

from dash_extensions.enrich import DashProxy, ServersideOutputTransform
from plotly_resampler.aggregation import MinMaxLTTB
from plotly_resampler import FigureResampler

# Global variables.
main_folder = "XFEL.SYNC"
ml01_folder = Path("C:/Users/Cherry_小龙虾/PycharmProjects/XFEL_PRIYA/XFEL.SYNC/LASER.LOCK.XLO/XTIN.MLO1")
app = DashProxy(__name__, external_stylesheets=[dbc.themes.LUX], transforms=[ServersideOutputTransform()])


# Function to read Parquet files from the ML01 subfolder.
def read_parquet_files(folder_path: Path) -> pd.DataFrame:
    start_time = time.time()
    files = os.listdir(folder_path)
    dataframes = []
    for file in files:
        if file.endswith(".parquet"):
            df = pd.read_parquet(folder_path / file)
            dataframes.append(df)
    end_time = time.time()
    print(f"Time taken to load files: {end_time - start_time} seconds")
    return pd.concat(dataframes, ignore_index=True)


# Function to construct the layout.
def serve_layout() -> dbc.Container:
    """Constructs the app's layout."""
    return dbc.Container(
        [
            dbc.Container(html.H1("Data loading and visualization dashboard"), style={"textAlign": "center"}),
            html.Hr(),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Select a subfolder of ML01:"),
                            dcc.Dropdown(
                                id="ml01-subfolder-dropdown",
                                options=[{"label": subfolder, "value": subfolder} for subfolder
                                         in os.listdir(ml01_folder)],
                                value=os.listdir(ml01_folder)[0] if os.listdir(ml01_folder) else None
                            ),
                        ],
                        md=4,
                    ),
                    dbc.Col(
                        [
                            dcc.Graph(id="graph-id", figure=go.Figure()),
                            dcc.Loading(dcc.Store(id="store")),
                        ],
                        md=8,
                    ),
                ],
                align="center",
            ),
        ],
        fluid=True,
    )


# Define layout.
app.layout = serve_layout()


# Callback to plot and update the graph.
@app.callback(
    [Output("graph-id", "figure"), Output("store", "data")],
    [Input("ml01-subfolder-dropdown", "value"), Input("graph-id", "relayoutData")],
    prevent_initial_call=True,
)
def update_graph(ml01_subfolder, n_clicks):
    start_time = time.time()
    if ml01_subfolder:
        ml01_subfolder_path = ml01_folder / ml01_subfolder
        ml01_data = read_parquet_files(ml01_subfolder_path)
        ml01_data['timestamp'] = pd.to_datetime(ml01_data['timestamp'], unit='s')
        fig_resampler = FigureResampler(
            go.Figure(),
            default_n_shown_samples=2_000,
            default_downsampler=MinMaxLTTB(),
        )
        fig_resampler.add_trace(go.Scatter(x=ml01_data['timestamp'], y=ml01_data['data'], mode='lines'))
        ml01_data_resampled = ml01_data.resample('10Min', on='timestamp').mean()
        fig = go.Figure(go.Scatter(x=ml01_data_resampled.index, y=ml01_data_resampled['data'], mode='lines'))
        fig.update_layout(
            title=f"Data vs Timestamp for {ml01_subfolder}",
            xaxis_title="Timestamp",
            yaxis_title="Data",
            title_x=0.5
        )
        end_time = time.time()
        print(f"Time taken to display graph: {end_time - start_time} seconds")
        return fig, ml01_data_resampled.to_json()
    else:
        return dash.no_update, dash.no_update


@app.callback(
    Output("graph-id", "figure", allow_duplicate=True),
    Input("graph-id", "relayoutData"),
    State("store", "data"),
    prevent_initial_call=True,
)
def update_fig(relayoutdata: dict, fig: FigureResampler):
    if fig is None:
        return no_update
    return fig.construct_update_data_patch(relayoutdata)


# Run the app.
if __name__ == "__main__":
    app.run_server(debug=True, port=9023, use_reloader=False)
