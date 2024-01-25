import os
import pandas as pd
import plotly.graph_objects as go
import dash
from dash import dcc
from dash import html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
from dash_bootstrap_templates import load_figure_template
from dash.exceptions import PreventUpdate


def read_parquet_files(subfolder_path, num_rows=100):
    files = [f for f in os.listdir(subfolder_path) if f.endswith('.parquet')]
    dataframes = [pd.read_table(os.path.join(subfolder_path, f), nrows=num_rows) for f in files]
    return pd.concat(dataframes, ignore_index=True)


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


def convert_timestamp(df):
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    return df


def downsample_data(df, timestamp_col='timestamp', value_col='data'):
    df_resampled = df.groupby(pd.Grouper(key=timestamp_col, freq='6h'))[value_col].mean().reset_index()
    return df_resampled


def update_line_plot(selected_subfolder, main_folders, sub_folders, subsub_folder):
    if selected_subfolder is None:
        return go.Figure()

    subfolder_path = os.path.join(main_folders, sub_folders, subsub_folder, selected_subfolder)
    df = read_parquet_files(subfolder_path)
    df = convert_timestamp(df)
    df = downsample_data(df)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['data'], mode='lines'))

    return fig


def create_layout(main_folders, sub_folders, subsub_folders_1, sub_folders2, subsub_folders_2, subsub_folders_3,
                  subsub_folders_4):
    return html.Div([
        html.H1("Dashboard"),
        html.Div([
            html.H2("XTIN.MLO1 and SLO1 Dashboards"),
            dbc.Row([
                dbc.Col([
                    dcc.Dropdown(
                        id='subfolder-dropdown',
                        options=get_dropdown_options(main_folders, sub_folders, subsub_folders_1),
                        value=None,
                        placeholder="Select Subfolder"
                    ),
                ], width=6),
                dbc.Col([
                    dcc.Dropdown(
                        id='subfolder-dropdown-2',
                        options=get_dropdown_options(main_folders, sub_folders, subsub_folders_2),
                        value=None,
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
            html.H2("LSU"),
            dbc.Row([
                dbc.Col([
                    dcc.Dropdown(
                        id='subfolder-dropdown-3',
                        options=get_dropdown_options(main_folders, sub_folders2, subsub_folders_3),
                        value=None,
                        placeholder="Select Subfolder"
                    ),
                ], width=6),
                dbc.Col([
                    dcc.Dropdown(
                        id='subfolder-dropdown-4',
                        options=get_dropdown_options(main_folders, sub_folders2, subsub_folders_4),
                        value=None,
                        placeholder="Select Subfolder"
                    ),
                ], width=6),
            ]),
            dbc.Row([
                dbc.Col(dcc.Loading(dcc.Graph(id='line-plot-3', config={'displayModeBar': False})), width=6),
                dbc.Col(dcc.Loading(dcc.Graph(id='line-plot-4', config={'displayModeBar': False})), width=6),
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
                           subsub_folder_4)


@app.callback(Output('line-plot-1', 'figure'),
              [Input('subfolder-dropdown', 'value'),
               Input('line-plot-1', 'relayoutData')])
def update_graph_1(selected_subfolder, relayout_data):
    if selected_subfolder is None:
        raise PreventUpdate
    if relayout_data is not None and 'xaxis.range[0]' in relayout_data:
        visible_range = relayout_data['xaxis.range[0]'], relayout_data['xaxis.range[1]']
        additional_rows = 100
        df_additional = read_parquet_files(subfolder_path, num_rows=additional_rows)
        df_additional = convert_timestamp(df_additional)
        df_resampled = pd.concat([df_resampled, downsample_data(df_additional)])
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_resampled['timestamp'], y=df_resampled['close'], mode='lines'))

    return fig


@app.callback(Output('line-plot-2', 'figure'), [Input('subfolder-dropdown-2', 'value')])
def update_graph_2(selected_subfolder):
    fig = update_line_plot(selected_subfolder, main_folder, sub_folder, subsub_folder_2)
    return fig


@app.callback(Output('line-plot-3', 'figure'), [Input('subfolder-dropdown-3', 'value')])
def update_graph_3(selected_subfolder):
    fig = update_line_plot(selected_subfolder, main_folder, sub_folder2, subsub_folder_3)
    return fig


@app.callback(Output('line-plot-4', 'figure'),
              [Input('subfolder-dropdown-4', 'value')])
def update_graph_4(selected_subfolder):
    fig = update_line_plot(selected_subfolder, main_folder, sub_folder2, subsub_folder_4)
    return fig


# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
