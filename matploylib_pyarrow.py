import os
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from tkinter import Tk, StringVar, OptionMenu, LabelFrame
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

# Function to read parquet files and convert timestamp
def read_parquet_and_convert_timestamp(directory):
    data = pd.DataFrame()
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".parquet"):
                filepath = os.path.join(root, file)
                df = pd.read_parquet(filepath)
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')  # Assuming timestamp is in seconds
                data = pd.concat([data, df])
    return data

# Function to update plot based on selected subsubsubfolders
def update_plot(*args):
    selected_xtin = xtin_var.get()
    selected_xhexp = xhexp_var.get()
    if selected_xtin and selected_xhexp:
        xtin_df = read_parquet_and_convert_timestamp(os.path.join('XFEL.SYNC', 'LASER.LOCK.XLO', 'XTIN.MLO1', selected_xtin))
        xhexp_df = read_parquet_and_convert_timestamp(os.path.join('XFEL.SYNC', 'LASER.LOCK.XLO', 'XHEXP1.SLO1', selected_xhexp))
        
        fig, ax = plt.subplots()
        ax.plot(xtin_df['timestamp'], xtin_df['data'], label='XTIN.MLO1')
        ax.plot(xhexp_df['timestamp'], xhexp_df['data'], label='XHEXP1.SLO1')
        ax.set_xlabel('Timestamp')
        ax.set_ylabel('Data')
        ax.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()

# GUI setup
root = Tk()
root.title("Dashboard")

# Dropdowns for selecting subsubsubfolders
subsubfolders_xtin = [d for d in os.listdir(os.path.join('XFEL.SYNC', 'LASER.LOCK.XLO', 'XTIN.MLO1')) if os.path.isdir(os.path.join('XFEL.SYNC', 'LASER.LOCK.XLO', 'XTIN.MLO1', d))]
subsubfolders_xhexp = [d for d in os.listdir(os.path.join('XFEL.SYNC', 'LASER.LOCK.XLO', 'XHEXP1.SLO1')) if os.path.isdir(os.path.join('XFEL.SYNC', 'LASER.LOCK.XLO', 'XHEXP1.SLO1', d))]

xtin_var = StringVar(root)
xtin_var.set(subsubfolders_xtin[0])  # default value
xhexp_var = StringVar(root)
xhexp_var.set(subsubfolders_xhexp[0])  # default value

xtin_dropdown = OptionMenu(root, xtin_var, *subsubfolders_xtin, command=update_plot)
xhexp_dropdown = OptionMenu(root, xhexp_var, *subsubfolders_xhexp, command=update_plot)

label_frame = LabelFrame(root, text="Select Subsubsubfolders")
label_frame.pack(padx=10, pady=10)
xtin_dropdown.pack(in_=label_frame, side="left")
xhexp_dropdown.pack(in_=label_frame, side="left")

# Initial plot
update_plot()

root.mainloop()
