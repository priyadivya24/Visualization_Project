import datetime
import os
import time
import pandas as pd
import plotly.express as px
import dash
from dash import dcc
from dash import html
import dash_bootstrap_components as dbc
from plotly.subplots import make_subplots
from dash.dependencies import Input, Output
from dash_bootstrap_templates import load_figure_template
import fastparquet


def read_parquet_files(subfolder_path):
    files = [f for f in os.listdir(subfolder_path) if f.endswith('.parquet')]
    tables = [fastparquet.ParquetFile(os.path.join(subfolder_path, f)).to_pandas() for f in files]
    return pd.concat(tables, ignore_index=True)


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


def get_initial_date_range(main_folders, sub_folders, subsub_folder):
    subsubsubfolder_path = os.path.join(main_folders, sub_folders, subsub_folder)
    files = [f for f in os.listdir(subsubsubfolder_path) if f.endswith('.parquet')]
    if not files:
        return None, None
    all_dates = []
    for file in files:
        file_path = os.path.join(subsubsubfolder_path, file)
        df = fastparquet.ParquetFile(file_path).to_pandas()
        all_dates.extend(df['timestamp'])
    min_date = min(all_dates)
    max_date = max(all_dates)
    return min_date, max_date


def convert_timestamp(df):
    now = datetime.now().strftime("%H:%M:%S")
    print("Convert Timestamp started : ", now)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    now = datetime.now().strftime("%H:%M:%S")
    print("Convert Timestamp completed at : ", now)
    return df


def downsample_data(df, timestamp_col='timestamp', value_col='data'):
    now = datetime.now().strftime("%H:%M:%S")
    print("Downsample Data started : ", now)
    df_resampled = df.groupby(pd.Grouper(key=timestamp_col, freq='1D'))[value_col].mean().reset_index()
    now = datetime.now().strftime("%H:%M:%S")
    print("Downsample Data completed at : ", now)
    return df_resampled


def update_line_plot(selected_subfolder, start_date, end_date, main_folders, sub_folders, subsub_folder):
    if selected_subfolder is None:
        return px.line()
    
    # Track time taken for execution
    start = time.perf_counter()
    now = datetime.now().strftime("%H:%M:%S")
    print("Read Parquet started : ", now)

    subfolder_path = os.path.join(main_folders, sub_folders, subsub_folder, selected_subfolder)
    df = read_parquet_files(subfolder_path)

    
    now = datetime.now().strftime("%H:%M:%S")
    print("Read Parquet completed at : ", now)

    df = convert_timestamp(df)
    if start_date and end_date:
        df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]
    elif start_date:
        df = df[df['timestamp'] >= start_date]
    elif end_date:
        df = df[df['timestamp'] >= end_date]
    df = downsample_data(df)

    fig = make_subplots(specs=[[{'secondary_y': True}]])
    fig.add_trace(px.line(df, x='timestamp', y='data').data[0])
    
    end = time.perf_counter()
    print(f'Update Line Plot Function Completed in {round(end - start, 2)} seconds')
    return fig


def create_layout(main_folders, sub_folders, subsub_folders_1, sub_folders2, subsub_folders_2, subsub_folders_3,
                  subsub_folders_4):
    return html.Div([
        html.H1("Dashboard"),
        html.Div([
            html.H2("XTIN.MLO1 and SLO1 Dashboards"),
            dcc.DatePickerRange(
                id='date1',
                display_format='YYYY-MM-DD',
                start_date=None,
                end_date=None,
                style={'margin-bottom': '20px'}
            ),
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
                dbc.Col(dcc.Graph(id='line-plot-1', config={'displayModeBar': False}), width=6),
                dbc.Col(dcc.Graph(id='line-plot-2', config={'displayModeBar': False}), width=6),
            ]),
        ]),
        html.Div([
            html.H2("LSU"),
            dcc.DatePickerRange(
                id='date2',
                display_format='YYYY-MM-DD',
                start_date=None,
                end_date=None,
                style={'margin-bottom': '20px'}
            ),
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
                dbc.Col(dcc.Graph(id='line-plot-3', config={'displayModeBar': False}), width=6),
                dbc.Col(dcc.Graph(id='line-plot-4', config={'displayModeBar': False}), width=6),
            ]),
        ]),
    ], style={'width': '80%', 'margin': 'auto', 'display': 'block'})


main_folder = 'XFEL.SYNC'
sub_folder = 'LASER.LOCK.XLO'
sub_folder2 = 'LINK.LOCK'
subsub_folder_1 = 'XTIN.MLO1'
subsub_folder_2 = 'XHEXP1.SLO1'
subsub_folder_3 = 'XTIN.AMC8.ACTUATOR'
subsub_folder_4 = 'XTIN.AMC8.CONTROLLER'

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.LUX])
load_figure_template('LUX')
app.layout = create_layout(main_folder, sub_folder, subsub_folder_1, sub_folder2, subsub_folder_2, subsub_folder_3,
                           subsub_folder_4, )


@app.callback(Output('line-plot-1', 'figure'),
              [Input('subfolder-dropdown', 'value'),
               Input('date1', 'start_date'),
               Input('date1', 'end_date')])
def update_graph_1(selected_subfolder, start_date, end_date):
    fig = update_line_plot(selected_subfolder, start_date, end_date, main_folder, sub_folder, subsub_folder_1)
    return fig


@app.callback(Output('line-plot-2', 'figure'),
              [Input('subfolder-dropdown-2', 'value'),
               Input('date1', 'start_date'),
               Input('date1', 'end_date')])
def update_graph_2(selected_subfolder, start_date, end_date):
    fig = update_line_plot(selected_subfolder, start_date, end_date, main_folder, sub_folder, subsub_folder_2)
    return fig


@app.callback(Output('line-plot-3', 'figure'),
              [Input('subfolder-dropdown-3', 'value'),
               Input('date2', 'start_date'),
               Input('date2', 'end_date')])
def update_graph_3(selected_subfolder, start_date, end_date):
    fig = update_line_plot(selected_subfolder, start_date, end_date, main_folder, sub_folder2, subsub_folder_3)
    return fig


@app.callback(Output('line-plot-4', 'figure'),
              [Input('subfolder-dropdown-4', 'value'),
               Input('date2', 'start_date'),
               Input('date2', 'end_date')])
def update_graph_4(selected_subfolder, start_date, end_date):
    fig = update_line_plot(selected_subfolder, start_date, end_date, main_folder, sub_folder2, subsub_folder_4)
    return fig


if __name__ == '__main__':
    app.run_server(debug=True)
