import os
import time
import pandas as pd
import matplotlib.pyplot as plt
from io import BytesIO
import base64
import datetime
import dash
from dash import dcc
from dash import html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output


def read_parquet_files(subfolder_path):
    files = [f for f in os.listdir(subfolder_path) if f.endswith('.parquet')]
    tables = [pd.read_parquet(os.path.join(subfolder_path, f)) for f in files]
    concatenated_df = pd.concat(tables)
    return concatenated_df


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
        return None

    start = time.perf_counter()
    print("Read Parquet started : ", datetime.datetime.now().strftime("%H:%M:%S"))

    subfolder_path = os.path.join(main_folders, sub_folders, subsub_folder, selected_subfolder)
    df = read_parquet_files(subfolder_path)
    print("Read Parquet completed at : ", datetime.datetime.now().strftime("%H:%M:%S"))

    df = convert_timestamp(df)
    df = downsample_data(df)

    fig, ax = plt.subplots()
    ax.plot(df['timestamp'], df['data'])
    ax.set_xlabel('Timestamp')
    ax.set_ylabel('Data')
    ax.set_title('Line Plot')

    end = time.perf_counter()
    print(f'Update Line Plot Function Completed in {round(end - start, 2)} seconds')

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
                dbc.Col(dcc.Loading(html.Div(id='line-plot-1')), width=6),
                dbc.Col(dcc.Loading(html.Div(id='line-plot-2')), width=6),
            ]),
        ]),
        html.Div([
            html.H2("LSU"),
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
                dbc.Col(dcc.Loading(html.Div(id='line-plot-3')), width=6),
                dbc.Col(dcc.Loading(html.Div(id='line-plot-4')), width=6),
            ]),
        ]),
    ], style={'width': '80%', 'margin': 'auto', 'display': 'block'})


def encode_image(fig):
    img = BytesIO()
    fig.savefig(img, format='png')
    encoded_img = base64.b64encode(img.getvalue()).decode('utf-8')
    return encoded_img


main_folder = 'XFEL.SYNC'
sub_folder = 'LASER.LOCK.XLO'
sub_folder2 = 'LINK.LOCK'
subsub_folder_1 = 'XTIN.MLO1'
subsub_folder_2 = 'XHEXP1.SLO1'
subsub_folder_3 = 'XTIN.AMC8.ACTUATOR'
subsub_folder_4 = 'XTIN.AMC8.CONTROLLER'

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.LUX])
app.layout = create_layout(main_folder, sub_folder, subsub_folder_1, sub_folder2, subsub_folder_2, subsub_folder_3,
                           subsub_folder_4)


@app.callback(Output('line-plot-1', 'children'), [Input('subfolder-dropdown', 'value')])
def update_graph_1(selected_subfolder):
    start = time.perf_counter()
    fig = update_line_plot(selected_subfolder, main_folder, sub_folder, subsub_folder_1)
    if fig:
        encoded_img = encode_image(fig)
        end = time.perf_counter()
        print(f'Update Graph1 Plot Function Completed in {round(end - start, 2)} seconds')
        return html.Img(src='data:image/png;base64,{}'.format(encoded_img))


@app.callback(Output('line-plot-2', 'children'), [Input('subfolder-dropdown-2', 'value')])
def update_graph_2(selected_subfolder):
    start = time.perf_counter()
    fig = update_line_plot(selected_subfolder, main_folder, sub_folder, subsub_folder_2)
    if fig:
        encoded_img = encode_image(fig)
        end = time.perf_counter()
        print(f'Update Graph2 Plot Function Completed in {round(end - start, 2)} seconds')
        return html.Img(src='data:image/png;base64,{}'.format(encoded_img))


@app.callback(Output('line-plot-3', 'children'), [Input('subfolder-dropdown-3', 'value')])
def update_graph_3(selected_subfolder):
    start = time.perf_counter()
    fig = update_line_plot(selected_subfolder, main_folder, sub_folder2, subsub_folder_3)
    if fig:
        encoded_img = encode_image(fig)
        end = time.perf_counter()
        print(f'Update Graph3 Plot Function Completed in {round(end - start, 2)} seconds')
        return html.Img(src='data:image/png;base64,{}'.format(encoded_img))


@app.callback(Output('line-plot-4', 'children'), [Input('subfolder-dropdown-4', 'value')])
def update_graph_4(selected_subfolder):
    start = time.perf_counter()
    fig = update_line_plot(selected_subfolder, main_folder, sub_folder2, subsub_folder_4)
    if fig:
        encoded_img = encode_image(fig)
        end = time.perf_counter()
        print(f'Update Graph4 Plot Function Completed in {round(end - start, 2)} seconds')
        return html.Img(src='data:image/png;base64,{}'.format(encoded_img))


if __name__ == '__main__':
    app.run_server(debug=True)
