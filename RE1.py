import datetime
import os
import time
import pandas as pd
import plotly.graph_objects as go
import dash
from dash import dcc, html, callback_context, no_update
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
import pyarrow.parquet as pq
from pathlib import Path
from typing import List
import pyarrow as pa
from dash_extensions.enrich import Output, Input, State
from dash_extensions.enrich import DashProxy, Serverside, ServersideOutputTransform
from plotly_resampler import FigureResampler
from trace_updater import TraceUpdater


# Function to read Parquet files.
def read_parquet_files(subfolder_path):
    files = [f for f in os.listdir(subfolder_path) if f.endswith('.parquet')]
    tables = [pq.read_table(os.path.join(subfolder_path, f)) for f in files]
    concatenated_table = pa.concat_tables(tables)
    return concatenated_table.to_pandas()


# Function to get dropdown options.
def get_dropdown_options(main_folders, sub_folders, subsub_folder):
    if None in (main_folders, sub_folders, subsub_folder):
        return []

    main_folders = str(main_folders)
    sub_folders = str(sub_folders)
    subsub_folder = str(subsub_folder)

    subsubsubfolder_path = os.path.join(main_folders, sub_folders, subsub_folder)
    subsubsubfolders = [f for f in os.listdir(subsubsubfolder_path) if
                        os.path.isdir(os.path.join(subsubsubfolder_path, f))]

    return [{'label': folder, 'value': folder} for folder in subsubsubfolders]


# Function to convert timestamp.
def convert_timestamp(df):
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    return df


# Function to downsample data.
def downsample_data(df, timestamp_col='timestamp', value_col='data'):
    df_resampled = df.groupby(pd.Grouper(key=timestamp_col, freq='6h'))[value_col].mean().reset_index()
    return df_resampled


# Function to update line plot.
def update_line_plot(selected_subfolder, main_folders, sub_folders, subsub_folder):
    if selected_subfolder is None:
        return go.Figure()
    start = time.perf_counter()
    now = datetime.datetime.now().strftime("%H:%M:%S")
    print("Read Parquet started : ", now)

    subfolder_path = os.path.join(main_folders, sub_folders, subsub_folder, selected_subfolder)
    df = read_parquet_files(subfolder_path)
    now = datetime.datetime.now().strftime("%H:%M:%S")
    print("Read Parquet completed at : ", now)
    df = convert_timestamp(df)
    df = downsample_data(df)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['data'], mode='lines'))
    end = time.perf_counter()
    print(f'Update Line Plot Function Completed in {round(end - start, 2)} seconds')

    return fig

def plot_graph(n_clicks, *folder_list):
    it = iter(folder_list)
    file_list: List[Path] = []
    for folder, files in zip(it, it):
        if not all((folder, files)):
            continue
        else:
            for file in files:
                file_list.append((Path(folder).joinpath(file)))
    print(file_list)

    ctx = callback_context
    if len(ctx.triggered) and "plot-button" in ctx.triggered[0]["prop_id"]:
        if len(file_list):
            fig: FigureResampler = visualize_multiple_files(file_list)  # Assuming visualize_multiple_files function is defined elsewhere.
            return fig, Serverside(fig)
    else:
        return no_update


# Function to create layout.
def create_layout(main_folders, sub_folders, subsub_folders_1, sub_folders2, subsub_folders_2, subsub_folders_3,
                  subsub_folders_4):
    return html.Div([
        html.H1("Data loading and visualization dashboard"),
        html.Div([
            html.H2("Example data"),
            dbc.Row([
                dbc.Col([
                    dcc.Dropdown(
                        id='subfolder-dropdown',
                        options=get_dropdown_options(main_folders, sub_folders, subsub_folders_1),
                        value='CTRL0.OUT.MEAN.RD',
                        placeholder="Select Subfolder"
                    ),
                ], width=6),
                dbc.Col([
                    dcc.Dropdown(
                        id='subfolder-dropdown-2',
                        options=get_dropdown_options(main_folders, sub_folders, subsub_folders_2),
                        value='CTRL0.OUT.MEAN.RD',
                        placeholder="Select Subfolder"
                    ),
                ], width=6),
            ]),
            dbc.Row([
                dbc.Col(dcc.Loading(dcc.Graph(id='line-plot-1', config={'displayModeBar': False})), width=6),
                dbc.Col(dcc.Loading(dcc.Graph(id='line-plot-2', config={'displayModeBar': False})), width=6),
            ]),
        ]),
        html.Div([
            html.H2("Other folder"),
            dbc.Row([
                dbc.Col([
                    dcc.Dropdown(
                        id='subfolder-dropdown-3',
                        options=get_dropdown_options(main_folders, sub_folders2, subsub_folders_3),
                        value='FMC1.MD22.0.POSITION.RD',
                        placeholder="Select Subfolder"
                    ),
                ], width=6),
                dbc.Col([
                    dcc.Dropdown(
                        id='subfolder-dropdown-4',
                        options=get_dropdown_options(main_folders, sub_folders2, subsub_folders_4),
                        value='LSU.2.CTRL_IN.MEAN.RD',
                        placeholder="Select Subfolder"
                    ),
                ], width=6),
            ]),
            dbc.Row([
                dbc.Col(dcc.Loading(dcc.Graph(id='line-plot-3', config={'displayModeBar': False})), width=6),
                dbc.Col(dcc.Loading(dcc.Graph(id='line-plot-4', config={'displayModeBar': False})), width=6),
            ]),
        ]),
        dbc.Container(id="graph-container", children=[
            dcc.Loading(dcc.Graph(id='graph-id', config={'displayModeBar': False})),
            dcc.Store(id='store'),
            TraceUpdater(id="trace-updater", gdID="graph-id")
        ]),
    ], style={'width': '80%', 'margin': 'auto', 'display': 'block'})


# Define main folder and subfolders.
main_folder = 'XFEL.SYNC'
sub_folder = 'LASER.LOCK.XLO'
sub_folder2 = 'LINK.LOCK'
subsub_folder_1 = 'XTIN.MLO1'
subsub_folder_2 = 'XHEXP1.SLO1'
subsub_folder_3 = 'XTIN.AMC8.ACTUATOR'
subsub_folder_4 = 'XTIN.AMC8.CONTROLLER'

# Initialize the Dash app.
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.LUX])
load_figure_template('LUX')

# Set the app layout.
app.layout = create_layout(main_folder, sub_folder, subsub_folder_1, sub_folder2, subsub_folder_2, subsub_folder_3,
                           subsub_folder_4)


# Callback to update the first graph.
@app.callback(Output('line-plot-1', 'figure'), [Input('subfolder-dropdown', 'value')])
def update_graph_1(selected_subfolder):
    start = time.perf_counter()
    fig = update_line_plot(selected_subfolder, main_folder, sub_folder, subsub_folder_1)
    end = time.perf_counter()
    print(f'Update Graph1 Plot Function Completed in {round(end - start, 2)} seconds')
    return fig


# Callback to update the second graph.
@app.callback(Output('line-plot-2', 'figure'), [Input('subfolder-dropdown-2', 'value')])
def update_graph_2(selected_subfolder):
    start = time.perf_counter()
    fig = update_line_plot(selected_subfolder, main_folder, sub_folder, subsub_folder_2)
    end = time.perf_counter()
    print(f'Update Graph2 Plot Function Completed in {round(end - start, 2)} seconds')
    return fig


# Callback to update the third graph.
@app.callback(Output('line-plot-3', 'figure'), [Input('subfolder-dropdown-3', 'value')])
def update_graph_3(selected_subfolder):
    start = time.perf_counter()
    fig = update_line_plot(selected_subfolder, main_folder, sub_folder2, subsub_folder_3)
    end = time.perf_counter()
    print(f'Update Graph3 Plot Function Completed in {round(end - start, 2)} seconds')
    return fig


# Callback to update the fourth graph.
@app.callback(Output('line-plot-4', 'figure'), [Input('subfolder-dropdown-4', 'value')])
def update_graph_4(selected_subfolder):
    start = time.perf_counter()
    fig = update_line_plot(selected_subfolder, main_folder, sub_folder2, subsub_folder_4)
    end = time.perf_counter()
    print(f'Update Graph4 Plot Function Completed in {round(end - start, 2)} seconds')
    return fig


if __name__ == '__main__':
    app.run_server(debug=True)
