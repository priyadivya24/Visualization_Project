import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time
import pandas as pd

def load_parquet_data(property_path, start_dt, stop_dt):
    """
    Parameters
    ----------
    base_path : Path
        The base directory path where DOOCS properties are stored.
    start_dt : datetime.datetime
        first datetime
    stop_dt : datetime.datetime
        last datetime

    Returns
    -------
    dictionary
        dictionary containing the requested data, a key for every doocs property path.
    """
    if not isinstance(property_path, Path):
        property_path = Path(property_path)

    start_timestamp = datetime.timestamp(start_dt)
    stop_timestamp = datetime.timestamp(stop_dt)

    required_months = []
    tmp_dt = datetime(start_dt.year, start_dt.month, 1)
    while tmp_dt < stop_dt:
        file_path = tmp_dt.strftime("%Y-%m") + ".parquet"  # Construct the file path
        required_months.append(file_path)
        tmp_dt += relativedelta(months=1)

    parquet_data = {}

    properties_mapping = get_doocs_properties(property_path)
    loading_start_time = time.time()

    for prop_path, prop_hierarchy in properties_mapping.items():
        prop_path = Path(prop_path)

        # Construct the ParquetDataset with filters
        dataset = pq.ParquetDataset(
            str(prop_path), 
            filters=[
                ('timestamp', '>=', start_timestamp), 
                ('timestamp', '<=', stop_timestamp)
            ]
        )
        parquet_data[prop_hierarchy] = dataset.read()

    loading_end_time = time.time()  # End time for data loading
    loading_runtime = loading_end_time - loading_start_time
    print("Data loading time:", loading_runtime, "seconds")
    return parquet_data

def get_doocs_properties(base_path):
    """
    Collects DOOCS properties and their corresponding paths.

    Parameters
    ----------
    base_path : Path
        The base directory path where DOOCS properties are stored.

    Returns
    -------
    dict
        A dictionary mapping property paths to their corresponding DOOCS hierarchy.
    """
    properties2paths = {}

    if base_path.is_dir():
        for fac_path in base_path.iterdir():
            fac = fac_path.name
            for dev_path in fac_path.iterdir():
                dev = dev_path.name
                for loc_path in dev_path.iterdir():
                    loc = loc_path.name
                    for prop_path in loc_path.iterdir():
                        prop = prop_path.name
                        properties2paths[str(prop_path)] = f"{fac}/{dev}/{loc}/{prop}"

    return properties2paths


base_path_str = "C:/Users/pmahad/Desktop/Project/XFEL.SYNC"
base_path = Path(base_path_str)  # Convert the string path to a Path object
properties_dict = get_doocs_properties(base_path)
start_dt = datetime(2023, 10, 1)
stop_dt = datetime(2023, 11, 10)

result = load_parquet_data(base_path, start_dt, stop_dt)

print(result)
for prop_path, doocs_hierarchy in properties_dict.items():
    print(f"Property Path: {prop_path}, DOOCS Hierarchy: {doocs_hierarchy}")
