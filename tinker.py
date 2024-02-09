import os
import pandas as pd
import matplotlib.pyplot as plt
from tkinter import *
from tkinter import filedialog
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import time


def read_parquet_files(subfolder_path):
    files = [f for f in os.listdir(subfolder_path) if f.endswith('.parquet')]
    tables = [pd.read_parquet(os.path.join(subfolder_path, f)) for f in files]
    concatenated_df = pd.concat(tables)
    return concatenated_df


def convert_timestamp(df):
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    return df


def downsample_data(df, timestamp_col='timestamp', value_col='data'):
    df_resampled = df.groupby(pd.Grouper(key=timestamp_col, freq='6h'))[value_col].mean().reset_index()
    return df_resampled


def update_line_plot():
    start_time = time.time()  # Start timer
    selected_subfolder = selected_subfolder_var.get()
    if selected_subfolder == '':
        return

    subfolder_path = os.path.join(main_folder.get(), sub_folder.get(), subsub_folder.get(), selected_subfolder)
    df = read_parquet_files(subfolder_path)
    df = convert_timestamp(df)
    df = downsample_data(df)

    fig, ax = plt.subplots()
    ax.plot(df['timestamp'], df['data'])
    ax.set_xlabel('Timestamp')
    ax.set_ylabel('Data')
    ax.set_title('Line Plot')

    canvas = FigureCanvasTkAgg(fig, master=frame)
    canvas.draw()
    canvas.get_tk_widget().pack(side=TOP, fill=BOTH, expand=1)

    end_time = time.time()  # End timer
    elapsed_time = end_time - start_time
    print(f"Graph updated in {elapsed_time:.2f} seconds")


def browse_button():
    start_time = time.time()  # Start timer
    filename = filedialog.askdirectory()
    folder_path.set(filename)
    end_time = time.time()  # End timer
    elapsed_time = end_time - start_time
    print(f"Folder selected in {elapsed_time:.2f} seconds")


root = Tk()
root.title('Matplotlib Dashboard')

frame = Frame(root)
frame.pack()

main_folder = StringVar()
sub_folder = StringVar()
subsub_folder = StringVar()
folder_path = StringVar()
selected_subfolder_var = StringVar(root)  # Store the selected subfolder

Label(frame, text="Main Folder:").grid(row=0, column=0)
Entry(frame, textvariable=main_folder).grid(row=0, column=1)
Button(frame, text="Browse", command=browse_button).grid(row=0, column=2)

Label(frame, text="Sub Folder:").grid(row=1, column=0)
Entry(frame, textvariable=sub_folder).grid(row=1, column=1)

Label(frame, text="Subsub Folder:").grid(row=2, column=0)
Entry(frame, textvariable=subsub_folder).grid(row=2, column=1)

Label(frame, text="Select Subfolder:").grid(row=3, column=0)
selected_subfolder_var.set('')  # Set default value
dropdown = OptionMenu(frame, selected_subfolder_var, '')  # Fixed here
dropdown.grid(row=3, column=1)

Button(frame, text="Update Plot", command=update_line_plot).grid(row=3, column=2)

root.mainloop()
