import os
import pandas as pd
import holoviews as hv
from datetime import datetime


# Function to read parquet files from subsubsubfolders
def read_parquet_files(folder_path):
    parquet_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".parquet"):
                parquet_files.append(os.path.join(root, file))
    return parquet_files


# Function to convert timestamp to normal date time
def convert_timestamp(timestamp):
    return datetime.fromtimestamp(timestamp)


# Main function to create dashboard
def create_dashboard():
    # Define main folder path
    main_folder = "XFEL.SYNC"

    # Define subsubsubfolders
    ml01_folders = ["XTIN.MLO1"]
    sl01_folders = ["XHEXP1.SLO1"]

    # Read data for ML01 and SL01
    ml01_data = []
    sl01_data = []

    for folder in ml01_folders:
        subsubsubfolders = os.listdir(os.path.join(main_folder, "LASER.LOCK.XLO", folder))
        for subfolder in subsubsubfolders:
            parquet_files = read_parquet_files(os.path.join(main_folder, "LASER.LOCK.XLO", folder, subfolder))
            for file in parquet_files:
                df = pd.read_parquet(str(file))
                df['timestamp'] = df['timestamp'].apply(convert_timestamp)
                ml01_data.append(df)

    for folder in sl01_folders:
        subsubsubfolders = os.listdir(os.path.join(main_folder, "LASER.LOCK.XLO", folder))
        for subfolder in subsubsubfolders:
            parquet_files = read_parquet_files(os.path.join(main_folder, "LASER.LOCK.XLO", folder, subfolder))
            for file in parquet_files:
                df = pd.read_parquet(str(file))
                df['timestamp'] = df['timestamp'].apply(convert_timestamp)
                sl01_data.append(df)

    # Concatenate dataframes
    ml01_data = pd.concat(ml01_data)
    sl01_data = pd.concat(sl01_data)

    # Create plots
    ml01_plot = ml01_data.hvplot.scatter(x='timestamp', y='data', xlabel='Timestamp', ylabel='Data', title='ML01').opts(
        width=600, height=400)
    sl01_plot = sl01_data.hvplot.scatter(x='timestamp', y='data', xlabel='Timestamp', ylabel='Data', title='SL01').opts(
        width=600, height=400)

    # Combine plots into a layout
    dashboard1 = (ml01_plot + sl01_plot).cols(1)

    # Return the dashboard
    return dashboard1


# Execute create_dashboard() function and assign the result to a variable
dashboard = create_dashboard()

# Display the dashboard
hv.extension('bokeh')
hv.renderer('bokeh').theme = 'caliber'
hv.save(dashboard, 'dashboard.html')
hv.show(dashboard)
