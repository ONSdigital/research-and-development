"""All general functions for the hdfs file system which uses PyDoop.
    These functions will need to be tested separately, using mocking.
"""

import pydoop.hdfs as hdfs
import pandas as pd
import json
import os

from src.data_validation.validation import validationlogger


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


def check_file_exists(filename: str, filepath: str = "./data/raw/") -> bool:
    """Checks if file exists on hdfs or locally and is non-empty.
    Raises an FileNotFoundError if the file doesn't exist.

    Keyword Arguments:
        filename -- Name of file to check
        filePath -- Relative path to file
        (default: {"./src/data_validation/validation.py"})

    Returns:
        A bool: boolean value is True if file exists and is non-empty,
        False otherwise.
    """
    output = False

    file_loc = os.path.join(filepath, filename)

    local_file = os.path.exists(file_loc)

    # If the file exists locally, check the size of it.
    if local_file:
        file_size = os.path.getsize(file_loc)

    # If file does not exists locally, check hdfs
    if not local_file:
        hdfs_file = hdfs_file_exists(file_loc)

        # If hdfs file exists, check its size
        if hdfs_file:
            file_size = hdfs_file_size(file_loc)

    # If file is not on hdfs but is local, and non-empty
    if local_file and file_size > 0:
        output = True
        validationlogger.info(f"File {filename} exists and is non-empty")

    # If file is empty, is not on hdfs but does exist locally
    elif local_file and file_size == 0:
        output = False
        validationlogger.warning(f"File {filename} exists but is empty")

    # If hdfs file exists and is non-empty
    elif hdfs_file and file_size > 0:
        output = True
        validationlogger.info(f"File {filename} exists on HDFS and is non-empty")

    # If hdfs file exists and is empty
    elif hdfs_file and file_size == 0:
        output = False
        validationlogger.warning(f"File {filename} exists on HDFS but is empty")

    # Raise error if file does not exist
    else:
        raise FileNotFoundError(f"File {filename} does not exist or is empty")

    return output
