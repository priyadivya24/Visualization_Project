import os
import pandas as pd
import matplotlib.pyplot as plt
from ipywidgets import interact, Dropdown


# Function to read parquet files and convert timestamp
def read_parquet_files(folder_path):
    df_list = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.parquet'):
                file_path = os.path.join(root, file)
                df = pd.read_parquet(str(file_path))
                df_list.append(df)
    return pd.concat(df_list)


# Main function to create dashboard
def create_dashboard(main1_folder_path):
    # Function to update plot based on dropdown selection
    def update_plot(selected_xtin_folder, selected_xhexp_folder):
        xtin_folder_data = read_parquet_files(
            os.path.join(main1_folder_path, 'LASER.LOCK.XLO', 'XTIN.MLO1', selected_xtin_folder))
        xhexp_folder_data = read_parquet_files(
            os.path.join(main1_folder_path, 'LASER.LOCK.XLO', 'XHEXP1.SLO1', selected_xhexp_folder))

        plt.plot(xtin_folder_data['timestamp'], xtin_folder_data['data'], label='XTIN.MLO1')
        plt.plot(xhexp_folder_data['timestamp'], xhexp_folder_data['data'], label='XHEXP1.SLO1')
        plt.xlabel('Time')
        plt.ylabel('Data')
        plt.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()

    # Create dropdown menu for subfolders in XTIN.MLO1
    xtins_folder_path = os.path.join(main_folder_path, 'LASER.LOCK.XLO', 'XTIN.MLO1')
    xhexps_folder_path = os.path.join(main_folder_path, 'LASER.LOCK.XLO', 'XHEXP1.SLO1')

    subfolders_xtin = os.listdir(xtins_folder_path)
    subfolders_xhexp = os.listdir(xhexps_folder_path)

    interact(update_plot, selected_xtin_folder=Dropdown(options=subfolders_xtin),
             selected_xhexp_folder=Dropdown(options=subfolders_xhexp))


# Specify the main folder path
main_folder_path = 'XFEL.SYNC'

# Create dashboard
create_dashboard(main_folder_path)
