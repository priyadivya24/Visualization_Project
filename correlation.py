from data import get_doocs_properties, load_parquet_data
from pathlib import Path
from datetime import datetime
from typing import List
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import MATCH, Input, Output, State, dcc, html, no_update, ClientsideFunction
from dash_extensions.enrich import (
    DashProxy,
    Serverside,
    ServersideOutputTransform,
    Trigger,
    TriggerTransform,
)
from dash.exceptions import PreventUpdate
import dash_table
from plotly_resampler import FigureResampler
from plotly_resampler.aggregation import MinMaxLTTB
import numpy as np
from scipy import signal
from plotly.subplots import make_subplots

# Data
online_data = {}
doocs_properties = get_doocs_properties(Path("C:/Users/pmahad/PycharmProjects/pythonProject/Database"))
app = DashProxy(__name__, transforms=[ServersideOutputTransform(), TriggerTransform()])

# Define your app layout
app.layout = html.Div([
    dcc.Tabs(id="tabs", value='XTIN', children=[
        dcc.Tab(label='XTIN', value='XTIN'),
        dcc.Tab(label='XHEXP1', value='XHEXP1'),
        dcc.Tab(label='Correlation Matrix', value='correlation_matrix'),
    ]),
    html.Div(id="tabs-content", children=[]),
    dcc.Dropdown(id="property-selecter", value='', placeholder="select LASER File(s)", multi=True),
    dcc.Dropdown(id="property-selecter1", value='', placeholder="select LSU File(s)", multi=True),
    dcc.Dropdown(id="property-selecter2", value='', placeholder="select CLIMATE File(s)", multi=True),
    html.Button("Load Data and Plot", id="load-plot"),
    html.Div(id="container", children=[]),
    html.Div(id="correlation-container", children=[
        html.Div(id="correlation-heatmap"),
        html.Div(id="correlation-data-table"),
    ]),
    html.Div(id="details-container", children=[]),
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
        properties = [prop for prop in doocs_properties.keys() if 'LASER.LOCK.XLO' in prop]
        properties1 = [prop for prop in doocs_properties.keys() if 'LINK.LOCK' in prop]
        properties2 = [prop for prop in doocs_properties.keys() if 'CLIMATE' in prop]
    return generate_dropdown_options(properties, properties1, properties2)


def generate_dropdown_options(properties=None, properties1=None, properties2=None):
    if properties and properties1 and properties2 is None:
        return []
    dropdown_options = [{'label': 'XFEL.SYNC' + prop.split('XFEL.SYNC')[1], 'value': prop} for prop in properties]
    dropdown_options1 = [{'label': 'XFEL.SYNC' + prop.split('XFEL.SYNC')[1], 'value': prop} for prop in properties1]
    dropdown_options2 = [{'label': 'XFEL.SYNC' + prop.split('XFEL.SYNC')[1], 'value': prop} for prop in properties2]
    return dropdown_options, dropdown_options1, dropdown_options2



@app.callback(
    Output("container", "children"),
    Input("load-plot", "n_clicks"),
    State("property-selecter", "value"),
    State("property-selecter1", "value"),
    State("property-selecter2", "value"),
    State("container", "children"),
    prevent_initial_call=True,
)
def add_graph_div(_, laser_files, link_files, climate_files, div_children: List[html.Div]):
    selected_properties = []
    selected_properties.extend(laser_files if laser_files else [])
    selected_properties.extend(link_files if link_files else [])
    selected_properties.extend(climate_files if climate_files else [])

    if len(selected_properties) < 2:
        raise PreventUpdate

    props = [Path(prop) for prop in selected_properties]
    loaded_data = load_parquet_data(props, datetime(2023, 10, 15, 17, 30), datetime(2023, 11, 15, 17, 30))

    # Clear previous graph divs
    div_children = []

    for key, item in loaded_data.items():
        online_data[doocs_properties[str(key)]] = item

    for dat in loaded_data:
        uid = doocs_properties[str(dat)]
        new_child = html.Div(
            children=[
                dcc.Loading(dcc.Store(id={"type": "store", "index": uid})),
                dcc.Interval(
                    id={"type": "interval", "index": uid}, max_intervals=1, interval=1
                ),
            ],
        )
        div_children.append(new_child)
    return div_children

@app.callback(
    [Output("correlation-heatmap", "children"),
     Output("correlation-data-table", "children")],
    Input("load-plot", "n_clicks"),
    State("property-selecter", "value"),
    State("property-selecter1", "value"),
    State("property-selecter2", "value"),
    prevent_initial_call=True,
)
def update_correlation_visualizations(n_clicks, laser_files, link_files, climate_files):
    selected_properties = []
    selected_properties.extend(laser_files if laser_files else [])
    selected_properties.extend(link_files if link_files else [])
    selected_properties.extend(climate_files if climate_files else [])

    if len(selected_properties) < 2:
        return html.Div(), html.Div()

    props = [Path(prop) for prop in selected_properties]
    loaded_data = load_parquet_data(props, datetime(2023, 10, 15, 17, 30), datetime(2023, 11, 15, 17, 30))
    combined_data = pd.concat([item.to_pandas().rename(columns={f"data": f"{doocs_properties[str(key)]}"}) for key, item in loaded_data.items()], axis=1)
    combined_data.drop(columns=['timestamp', 'bunchID'], inplace=True, errors='ignore')
    # Pandas correlation matrix
    correlation_matrix = combined_data.corr()

    # correlation matrix map
    fig_heatmap = go.Figure(data=go.Heatmap(
        z=correlation_matrix.values,
        x=correlation_matrix.columns,
        y=correlation_matrix.columns,
        colorscale='Viridis',
        colorbar=dict(title='Correlation'),
    ))

    annotations = []
    for i, row in enumerate(correlation_matrix.values):
        for j, value in enumerate(row):
            annotations.append(
                dict(
                    x=correlation_matrix.columns[j],
                    y=correlation_matrix.columns[i],
                    text=str(round(value, 2)),
                    xref='x',
                    yref='y',
                    font=dict(color='white'),
                    showarrow=False)
            )

    fig_heatmap.update_layout(
        title='Correlation Matrix',
        xaxis_title='Variables',
        yaxis_title='Variables',
        annotations=annotations
    )

    correlation_heatmap = dcc.Graph(figure=fig_heatmap)

    # DataTable to display correlation matrix values
    correlation_data_table = dash_table.DataTable(
        id='correlation-data',
        columns=[{'name': col, 'id': col} for col in correlation_matrix.columns],
        data=correlation_matrix.to_dict('records'),
        style_table={'overflowX': 'auto'},
    )

    return correlation_heatmap, correlation_data_table

@app.callback(
    Output("details-container", "children"),
    Input("load-plot", "n_clicks"),
    State("property-selecter", "value"),
    State("property-selecter1", "value"),
    State("property-selecter2", "value"),
    prevent_initial_call=True,
)
def update_details(n_clicks, laser_files, link_files, climate_files):
    selected_properties = []
    selected_properties.extend(laser_files if laser_files else [])
    selected_properties.extend(link_files if link_files else [])
    selected_properties.extend(climate_files if climate_files else [])

    if not selected_properties:
        return html.Div()

    props = [Path(prop) for prop in selected_properties]
    loaded_data = load_parquet_data(props, datetime(2023, 10, 15, 17, 30), datetime(2023, 11, 15, 17, 30))

    details_content = []

    for key, item in loaded_data.items():

        item_df = item.to_pandas()
        item_df.index = pd.to_datetime(item_df.index)
        daily_data = item_df.groupby(item_df.index.date)
        daily_stats = []

        for date, group in daily_data:

            mean_value = group['data'].mean()
            min_value = group['data'].min()
            max_value = group['data'].max()
            std_dev = group['data'].std()  # Calculate standard deviation

            min_timestamp = group['data'].idxmin()
            max_timestamp = group['data'].idxmax()

            # statics
            daily_stats.append(html.Div([
                html.H3(f"{doocs_properties[str(key)]}"),
                html.Table([
                    html.Tr([html.Th('Statistic'), html.Th('Value')]),
                    html.Tr([html.Td('Mean'), html.Td(round(mean_value, 2))]),
                    html.Tr([html.Td('Min'), html.Td(round(min_value, 2))]),
                    html.Tr([html.Td('Max'), html.Td(round(max_value, 2))]),
                    html.Tr([html.Td('Std Dev'), html.Td(round(std_dev, 2))]),
                ])
            ]))
        details_content.extend(daily_stats)

    return details_content





# Running the app
if __name__ == "__main__":
    app.run_server(debug=True, port=9023)
