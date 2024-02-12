import os
import pandas as pd
from bokeh.io import curdoc
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, Select
from bokeh.plotting import figure
from datetime import datetime

# main folder and subfolders path
main_folder = "XFEL.SYNC"
subfolders = {
    "LASER.LOCK.XLO": ["XHEXP1.SLO1", "XTIN.MLO1"],
    "LINK.LOCK": ["XTIN.AMC8.ACTUATOR", "XTIN.AMC8.CONTROLLER"]
}


# load data from parquet files
def load_data(subsubfolder):
    data = pd.DataFrame(columns=["timestamp", "data"])
    for root, dirs, files in os.walk(subsubfolder):
        for file in files:
            if file.endswith(".parquet"):
                df = pd.read_parquet(str(os.path.join(root, file)))
                data = pd.concat([data, df], ignore_index=True)
    return data


# convert timestamp to datetime
def convert_timestamp(timestamp):
    return datetime.fromtimestamp(timestamp / 1000)


# Create ColumnDataSource
source = ColumnDataSource(data=dict(timestamp=[], data=[]))

# dropdowns
ml01_select = Select(title="ML01", value="Select ML01 folder", options=["Select ML01 folder"])
sl01_select = Select(title="SL01", value="Select SL01 folder", options=["Select SL01 folder"])
actu_select = Select(title="Acuator", value="Select Actuator folder", options=["Select Actuator folder"])
cont_select = Select(title="Controller", value="Select Controller folder", options=["Select Controller folder"])

# plots
plot_ml01 = figure(title="ML01 Data", x_axis_label="Timestamp", y_axis_label="Data", x_axis_type="datetime")
plot_sl01 = figure(title="SL01 Data", x_axis_label="Timestamp", y_axis_label="Data", x_axis_type="datetime")
plot_actu = figure(title="Actuator Data", x_axis_label="Timestamp", y_axis_label="Data", x_axis_type="datetime")
plot_cont = figure(title="Controller Data", x_axis_label="Timestamp", y_axis_label="Data", x_axis_type="datetime")


# Update data source based on selection
def update_data():
    selected_ml01 = ml01_select.value
    selected_sl01 = sl01_select.value
    selected_actu = actu_select.value
    selected_cont = cont_select.value

    if selected_ml01 != "Select ML01 folder":
        data_ml01 = load_data(selected_ml01)
        data_ml01["timestamp"] = data_ml01["timestamp"].apply(convert_timestamp)
        plot_ml01.line(x="timestamp", y="data", source=ColumnDataSource(data_ml01))

    if selected_sl01 != "Select SL01 folder":
        data_sl01 = load_data(selected_sl01)
        data_sl01["timestamp"] = data_sl01["timestamp"].apply(convert_timestamp)
        plot_sl01.line(x="timestamp", y="data", source=ColumnDataSource(data_sl01))

    if selected_actu != "Select Actuator folder":
        data_actu = load_data(selected_actu)
        data_actu["timestamp"] = data_actu["timestamp"].apply(convert_timestamp)
        plot_actu.line(x="timestamp", y="data", source=ColumnDataSource(data_actu))

    if selected_cont != "Select Controller folder":
        data_cont = load_data(selected_cont)
        data_cont["timestamp"] = data_cont["timestamp"].apply(convert_timestamp)
        plot_cont.line(x="timestamp", y="data", source=ColumnDataSource(data_cont))


# Update data source on dropdown change
ml01_select.on_change('value', lambda attr, old, new: update_data())
sl01_select.on_change('value', lambda attr, old, new: update_data())
actu_select.on_change('value', lambda attr, old, new: update_data())
cont_select.on_change('value', lambda attr, old, new: update_data())

# Initialize plots
update_data()

# Add plots and dropdowns to the layout
layout = column(ml01_select, plot_ml01, sl01_select, plot_sl01)

# Add layout to current document
curdoc().add_root(layout)
