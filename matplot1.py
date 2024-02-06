import os
import pandas as pd
import matplotlib.pyplot as plt
from ipywidgets import interact, fixed


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


# Function to create plot for XTIN.MLO1
def create_xtin_plot(main1_folder_path, selected_folder):
    xtin_folder_data = read_parquet_files(
        os.path.join(main1_folder_path, 'LASER.LOCK.XLO', 'XTIN.MLO1', selected_folder))
    plt.plot(xtin_folder_data['timestamp'], xtin_folder_data['data'],
             label='XTIN.MLO1 - ' + selected_folder[:10])  # Only take first 10 characters of folder name
    plt.xlabel('Time')
    plt.ylabel('Data')
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Display legend outside the plot area
    plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))

    plt.show()


# Function to create plot for XHEXP1.SLO1
def create_xhexp_plot(main_folders_path, selected_folder):
    xhexp_folder_data = read_parquet_files(
        os.path.join(main_folders_path, 'LASER.LOCK.XLO', 'XHEXP1.SLO1', selected_folder))
    plt.plot(xhexp_folder_data['timestamp'], xhexp_folder_data['data'],
             label='XHEXP1.SLO1 - ' + selected_folder[:10])  # Only take first 10 characters of folder name
    plt.xlabel('Time')
    plt.ylabel('Data')
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Display legend outside the plot area
    plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))

    plt.show()


# Main function to create dashboard
def create_dashboard(main_folder_paths):
    # Create dropdown menu for subfolders in XTIN.MLO1 and XHEXP1.SLO1
    xtins_folder_path = os.path.join(main_folder_paths, 'LASER.LOCK.XLO', 'XTIN.MLO1')
    xhexps_folder_path = os.path.join(main_folder_paths, 'LASER.LOCK.XLO', 'XHEXP1.SLO1')

    subfolders_xtin = os.listdir(xtins_folder_path)
    subfolders_xhexp = os.listdir(xhexps_folder_path)

    # Create interactive widgets for selecting folders
    interact(create_xtin_plot, main_folder_path=fixed(main_folder_path), selected_folder=subfolders_xtin)
    interact(create_xhexp_plot, main_folder_path=fixed(main_folder_path), selected_folder=subfolders_xhexp)


# Specify the main folder path
main_folder_path = 'XFEL.SYNC'

# Create dashboard
create_dashboard(main_folder_path)
