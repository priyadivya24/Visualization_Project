import os
import pandas as pd
import plotly.express as px
import dash
from dash import dcc
from dash import html
import dash_bootstrap_components as dbc
from plotly.subplots import make_subplots
from dash.dependencies import Input, Output
from dash_bootstrap_templates import load_figure_template
from dash import dash_table


# Read parquet files with pagination
def read_parquet_files_paged(subfolder_path, page_current, page_size):
    files = [f for f in os.listdir(subfolder_path) if f.endswith('.parquet')]
    dataframes = [pd.read_parquet(os.path.join(subfolder_path, f)) for f in files]
    df = pd.concat(dataframes, ignore_index=True)
    start_index = page_current * page_size
    end_index = start_index + page_size
    return df.iloc[start_index:end_index]


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


# convert timestamp
def convert_timestamp(df):
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    return df


# downsample data using plotly resampler
def downsample_data(df):
    df = df.set_index('timestamp')
    df_resampled = df.resample('1d').mean()
    df_resampled = df_resampled.reset_index()
    return df_resampled


# update plot
def update_line_plot(selected_subfolder, main_folders, sub_folders, subsub_folder):
    if selected_subfolder is None:
        return px.line()

    subfolder_path = os.path.join(main_folders, sub_folders, subsub_folder, selected_subfolder)
    df = read_parquet_files_paged(subfolder_path, 0, 100)
    df = convert_timestamp(df)
    df = downsample_data(df)

    fig = make_subplots(specs=[[{'secondary_y': True}]])
    fig.add_trace(px.line(df, x='timestamp', y='data').data[0])

    return fig


# Callback to update DataTable based on selected subfolder and page


# ... (existing code)

# Add DataTable to layout
def create_layout(main_folders, sub_folders, subsub_folders_1, sub_folders_2, subsub_folders_2, subsub_folders_3,
                  subsub_folders_4):
    return html.Div([
        html.H1("Dashboard"),
        html.Div([
            html.H2("MLO1 and SLO1"),
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
                dbc.Col(dcc.Graph(id='line-plot-1', config={'displayModeBar': False}), width=6),
                dbc.Col(dcc.Graph(id='line-plot-2', config={'displayModeBar': False}), width=6),
            ]),
        ]),
        html.Div([
            html.H2("LSU"),
            dbc.Row([
                dbc.Col([
                    dcc.Dropdown(
                        id='subfolder-dropdown-3',
                        options=get_dropdown_options(main_folders, sub_folders_2, subsub_folders_3),
                        value=None,
                        placeholder="Select Subfolder"
                    ),
                ], width=6),
                dbc.Col([
                    dcc.Dropdown(
                        id='subfolder-dropdown-4',
                        options=get_dropdown_options(main_folders, sub_folders_2, subsub_folders_4),
                        value=None,
                        placeholder="Select Subfolder"
                    ),
                ], width=6),
            ]),
            dbc.Row([
                dbc.Col(dcc.Graph(id='line-plot-3', config={'displayModeBar': False}), width=6),
                dbc.Col(dcc.Graph(id='line-plot-4', config={'displayModeBar': False}), width=6),
            ]),
            dbc.Row([
                dbc.Col(dash_table.DataTable(
                    id='data-table',
                    columns=[{'name': col, 'id': col} for col in ['timestamp', 'data']],
                    # Adjust column names as needed
                    page_size=100,
                    page_current=0,
                ), width=12),
            ]),
        ]),
    ], style={'width': '80%', 'margin': 'auto', 'display': 'block'})


# hardcode path
main_folder = 'XFEL.SYNC'
sub_folder = 'LASER.LOCK.XLO'
sub_folder_2 = 'LINK.LOCK'
subsub_folder_1 = 'XTIN.MLO1'
subsub_folder_2 = 'XHEXP1.SLO1'
subsub_folder_3 = 'XTIN.AMC8.ACTUATOR'
subsub_folder_4 = 'XTIN.AMC8.CONTROLLER'

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.LUX])
load_figure_template('LUX')
app.layout = create_layout(main_folder, sub_folder, subsub_folder_1, sub_folder_2, subsub_folder_2, subsub_folder_3,
                           subsub_folder_4)


@app.callback(
    [Output('line-plot-1', 'figure'),
     Output('data-table', 'data')],
    [Input('subfolder-dropdown', 'value'),
     Input('data-table', "page_current")]
)
def update_graph_1_and_table(selected_subfolder, page_current):
    fig = update_line_plot(selected_subfolder, main_folder, sub_folder, subsub_folder_1)

    if selected_subfolder is None:
        return fig, []

    subfolder_path = os.path.join(main_folder, sub_folder, subsub_folder_1, selected_subfolder)
    df_page = read_parquet_files_paged(subfolder_path, page_current, PAGE_SIZE)
    return fig, df_page.to_dict('records')


@app.callback(Output('line-plot-2', 'figure'),
              [Input('subfolder-dropdown-2', 'value')])
def update_graph_2(selected_subfolder):
    fig = update_line_plot(selected_subfolder, main_folder, sub_folder, subsub_folder_2)
    return fig


@app.callback(Output('line-plot-3', 'figure'),
              [Input('subfolder-dropdown-3', 'value')])
def update_graph_3(selected_subfolder):
    fig = update_line_plot(selected_subfolder, main_folder, sub_folder_2, subsub_folder_3)
    return fig


@app.callback(Output('line-plot-4', 'figure'),
              [Input('subfolder-dropdown-4', 'value')])
def update_graph_4(selected_subfolder):
    fig = update_line_plot(selected_subfolder, main_folder, sub_folder_2, subsub_folder_4)
    return fig


PAGE_SIZE = 100

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
