import os
import pandas as pd
from bokeh.io import curdoc
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, Select
from bokeh.plotting import figure
from datetime import datetime


# Define the main folder and subfolders
main_folder = "XFEL.SYNC"
subfolders = {
    "LASER.LOCK.XLO": ["ML01", "SL01"],
    "LINK.LOCK": ["XTIN.AMC8.ACTUATOR", "XTIN.AMC8.CONTROLLER"]
}


# Function to load data from parquet files
def load_data(subsubfolder):
    data = pd.DataFrame(columns=["timestamp", "data"])
    for root, dirs, files in os.walk(subsubfolder):
        for file in files:
            if file.endswith(".parquet"):
                df = pd.read_parquet(os.path.join(root, file))
                data = pd.concat([data, df], ignore_index=True)
    return data


# Function to convert timestamp to datetime
def convert_timestamp(timestamp):
    return datetime.fromtimestamp(timestamp / 1000)


# Create ColumnDataSource
source = ColumnDataSource(data=dict(timestamp=[], data=[]))

# Create dropdowns for subsubsubfolders
ml01_select = Select(title="ML01", value="Select ML01 folder", options=["Select ML01 folder"])
sl01_select = Select(title="SL01", value="Select SL01 folder", options=["Select SL01 folder"])

# Create plots
plot_ml01 = figure(title="ML01 Data", x_axis_label="Timestamp", y_axis_label="Data", x_axis_type="datetime")
plot_sl01 = figure(title="SL01 Data", x_axis_label="Timestamp", y_axis_label="Data", x_axis_type="datetime")


# Update data source based on selection
def update_data(attrname, old, new):
    selected_ml01 = ml01_select.value
    selected_sl01 = sl01_select.value

    if selected_ml01 != "Select ML01 folder":
        data_ml01 = load_data(selected_ml01)
        data_ml01["timestamp"] = data_ml01["timestamp"].apply(convert_timestamp)
        plot_ml01.line(x="timestamp", y="data", source=ColumnDataSource(data_ml01))

    if selected_sl01 != "Select SL01 folder":
        data_sl01 = load_data(selected_sl01)
        data_sl01["timestamp"] = data_sl01["timestamp"].apply(convert_timestamp)
        plot_sl01.line(x="timestamp", y="data", source=ColumnDataSource(data_sl01))


# Update data source on dropdown change
ml01_select.on_change('value', update_data)
sl01_select.on_change('value', update_data)

# Initialize plots
update_data(None, None, None)

# Add plots and dropdowns to the layout
layout = column(ml01_select, plot_ml01, sl01_select, plot_sl01)

# Add layout to current document
curdoc().add_root(layout)
