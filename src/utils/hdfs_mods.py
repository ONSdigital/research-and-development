"""All general functions for the hdfs file system which uses PyDoop.
    These functions will need to be tested separately, using mocking.
"""

import pydoop.hdfs as hdfs
import pandas as pd
import json


def read_hdfs_csv(filepath: str) -> pd.DataFrame:
    """Reads a csv from DAP into a Pandas Dataframe
    Args:
        filepath (str): Filepath (Specified in config)

    Returns:
        pd.DataFrame: Dataframe created from csv
    """
    # Open the file in read mode inside Hadoop context
    with hdfs.open(filepath, "r") as file:
        # Import csv file and convert to Dataframe
        df_imported_from_hdfs = pd.read_csv(file)

    return df_imported_from_hdfs


def write_hdfs_csv(filepath: str, data: pd.DataFrame):
    """Writes a Pandas Dataframe to csv in DAP

    Args:
        filepath (str): Filepath (Specified in config)
        data (pd.DataFrame): Data to be stored
    """
    # Open the file in write mode
    with hdfs.open(filepath, "wt") as file:
        # Write dataframe to DAP context
        data.to_csv(file, index=False)
    return None


def hdfs_load_json(filepath: str) -> dict:
    """Function to load JSON data from DAP
    Args:
        filepath (string): The filepath in Hue
    """

    # Open the file in read mode inside Hadoop context
    with hdfs.open(filepath, "r") as file:
        # Import csv file and convert to Dataframe
        datadict = json.load(file)

    return datadict


def hdfs_file_exists(filepath: str) -> bool:
    """Function to check file exists

    Args:
        filepath (string) -- The filepath in Hue

    Returns:
        Bool - A boolean value indicating whether a file
        exists or not
    """
    file_exists = hdfs.path.exists(filepath)

    return file_exists


def hdfs_file_size(filepath: str) -> int:
    """Function to check file exists

    Args:
        filepath (string) -- The filepath in Hue

    Returns:
        Int - an integer value indicating the size
        of the file in bytes
    """
    file_size = hdfs.path.getsize(filepath)

    return file_size
