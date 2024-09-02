"""Read in the large postcode lookup and save smaller csv with two required columns.

NOTE: This module is NOT used as part of the main pipeline, but can be called via the
postcode_reducer_main.py script at the highest level of the project.

The input is a very large csv file with many columns. The output is a smaller csv file
with only the required 2 columns, which will be saved in the relevant mappers folder.

Paths and file names as well as the survey year are read in from the config files.
"""
import pandas as pd

from typing import Callable
from datetime import datetime

from src.utils.config import config_setup


def run_postcode_reduction(user_config_path, dev_config_path):

    # load and validate the config
    config = config_setup(user_config_path, dev_config_path)

    # Check the environment switch
    network_or_hdfs = config["global"]["network_or_hdfs"]

    if network_or_hdfs == "network":
        from src.utils import local_file_mods as mods

    elif network_or_hdfs == "hdfs":
        from src.utils import hdfs_mods as mods

    else:
        print("The network_or_hdfs configuration is wrong")
        raise ImportError

    # return a dictionary of paths suitable for the current environment
    paths = config[f"{network_or_hdfs}_paths"]

    df = input_large_csv(paths, mods.rd_file_exists, mods.rd_read_csv)
    print("Postcode mapper csv read in.")

    output_large_csv(df, paths, mods.rd_write_csv)
    print("Postcode mapper csv written to new location.")


def input_large_csv(
    paths: dict,
    file_exists_func: Callable,
    read_csv_func: Callable,
) -> pd.DataFrame:
    """Set up file paths and read in required columns of large postcode lookup file.

    Args:
        paths (dict): A dictionary of paths for the current environment.

    Returns:
        pd.DataFrame: The postcode lookup file with only the required columns only.
    """
    start_time = datetime.now()
    # Input and output folder and file names
    in_file = paths["postcode_masterlist"]

    # check the input paths are valid
    file_exists_func(in_file)

    # The required columns
    key_cols = ["pcd2", "itl"]

    # read in the postcode lookup file
    print(f"Reading the postcode  lookup file {in_file}...")
    df = read_csv_func(in_file, usecols=key_cols)

    time_taken = (datetime.now() - start_time).total_seconds()
    print(f"Time taken to read in postcode lookup file: {time_taken} seconds")

    return df


def output_large_csv(
    df: pd.DataFrame,
    paths: dict,
    write_csv_func: Callable,
) -> None:
    """Write the reduced postcode lookup file to the new destination.

    Args:
        df (pd.DataFrame): The postcode lookup file with only the required columns only.
        paths (dict): A dictionary of paths for the current environment.
        write_csv_func (Callable): The function to write the csv file.

    Returns:
        None
    """
    start_time = datetime.now()

    survey_year = paths["year"]

    # Output folder and file names
    out_fol = paths["mappers"]
    out_path = out_fol + f"postcodes_{survey_year}.csv"

    # write the reduced file to the new destination
    print(f"Saving the postcode mapper file {out_path}...")
    write_csv_func(out_path, df)

    print("file saved to " + out_fol)

    time_taken = (datetime.now() - start_time).total_seconds()
    print(f"Time taken to write out postcode lookup file: {time_taken} seconds")
