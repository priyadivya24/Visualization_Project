import dash
from dash import dcc, html
from dash.dependencies import Input, Output
from data import load_parquet_data, get_doocs_properties
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime
import time
import pandas as pd

# Load data from data.py
online_data = {}
doocs_properties = get_doocs_properties(Path("C:/Users/pmahad/PycharmProjects/pythonProject/Database"))

app = dash.Dash(__name__)

# Callback to update content based on selected tab
@app.callback(
    Output('property-dropdown', 'options'),
    [Input('tabs', 'value'),
     Input('file-type-dropdown', 'value')]
)
def update_dropdown_options(tab, file_type):
    if tab == 'tab-xtin':
        properties = [prop for prop in doocs_properties.keys() if 'XTIN' in prop and file_type in prop]
    elif tab == 'tab-xhexp1':
        properties = [prop for prop in doocs_properties.keys() if 'XHEXP1' in prop and file_type in prop]
    else:
        properties = []
    return generate_dropdown_options(properties)


def generate_dropdown_options(properties=None):
    if properties is None:
        return []
    dropdown_options = [{'label': 'XFEL.SYNC' + prop.split('XFEL.SYNC')[1], 'value': prop} for prop in properties]
    return dropdown_options


# Callback to update graph based on property selection
@app.callback(
    Output('property-graph-container', 'children'),
    [Input('load-plot-button', 'n_clicks')],
    [Input('property-dropdown', 'value')]
)
def load_and_plot_data(n_clicks, selected_properties):
    start_time = time.time()
    graphs = []
    if n_clicks > 0 and selected_properties:
        for selected_property in selected_properties:
            prop_path = Path(selected_property)
            loaded_data = load_parquet_data(prop_path, datetime(2023, 10, 15, 17, 30), datetime(2023, 11, 15, 17, 30))
            online_data[doocs_properties[str(prop_path)]] = loaded_data[prop_path]

            # Get data for the selected property
            data_table = online_data[doocs_properties[str(prop_path)]]
            data_df = data_table.to_pandas()

            # Convert timestamp column to datetime
            data_df['timestamp'] = pd.to_datetime(data_df['timestamp'], unit='s')

            # Downsample the data
            data_df.set_index('timestamp', inplace=True)
            data_resampled = data_df.resample('1d').mean().reset_index()

            # Create graph
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=data_resampled['timestamp'], y=data_resampled['data'], mode='lines', name=doocs_properties[str(prop_path)]))
            graphs.append(html.Div(dcc.Loading(dcc.Graph(figure=fig))))

    end_time = time.time()
    total_time = end_time - start_time
    print(f"Total time taken: {total_time} seconds")
    return graphs


# Define layout
app.layout = html.Div([
    html.H1("Dashboard"),
    dcc.Tabs(id='tabs', value='tab-xtin', children=[
        dcc.Tab(label='XTIN', value='tab-xtin'),
        dcc.Tab(label='XHEXP1', value='tab-xhexp1')
    ]),
    html.Div(id='tabs-content'),
    html.Label('Select File Property:'),
    dcc.Dropdown(
        id='file-type-dropdown',
        options=[
            {'label': 'CLIMATE', 'value': 'CLIMATE'},
            {'label': 'LINK.LOCK', 'value': 'LINK.LOCK'},
            {'label': 'LASER.LOCK.XLO', 'value': 'LASER.LOCK.XLO'}
        ],
        value=''
    ),
    html.Label('Select File(s):'),
    dcc.Dropdown(
        id='property-dropdown',
        value='',
        multi=True
    ),
    html.Button('Load and Plot', id='load-plot-button', n_clicks=0),
    html.Div(id='property-graph-container'),
])

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
