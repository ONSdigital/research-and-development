"""All general functions for the hdfs file system which uses PyDoop.
    These functions will need to be tested separately, using mocking.
"""

import pandas as pd
import json
import logging
from typing import List

from src.utils.wrappers import time_logger_wrap

try:
    import pydoop.hdfs as hdfs

    # from src.utils.hdfs_mods import read_hdfs_csv, write_hdfs_csv
    HDFS_AVAILABLE = True
except ImportError:
    HDFS_AVAILABLE = False

# set up logging
hdfs_logger = logging.getLogger(__name__)


def read_hdfs_csv(filepath: str, cols: List[str] = None) -> pd.DataFrame:
    """Reads a csv from DAP into a Pandas Dataframe
    Args:
        filepath (str): Filepath (Specified in config)
        cols (List[str]): Optional list of columns to be read in
    Returns:
        pd.DataFrame: Dataframe created from csv
    """
    # Open the file in read mode inside Hadoop context
    with hdfs.open(filepath, "r") as file:
        # Import csv file and convert to Dataframe
        if not cols:
            df_from_hdfs = pd.read_csv(file)
        else:
            try:
                df_from_hdfs = pd.read_csv(file, usecols=cols)
            except Exception:
                hdfs_logger.error(f"Could not find specified columns in {filepath}")
                hdfs_logger.info("Columns specified: " + str(cols))
                raise ValueError
    return df_from_hdfs


def write_hdfs_csv(filepath: str, data: pd.DataFrame):
    """Writes a Pandas Dataframe to csv in DAP

    Args:
        filepath (str): Filepath (Specified in config)
        data (pd.DataFrame): Data to be stored
    """
    # Open the file in write mode
    with hdfs.open(filepath, "wt") as file:
        # Write dataframe to DAP context
        data.to_csv(file, date_format="%Y-%m-%d %H:%M:%S.%f+00", index=False)
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
    """Function to check file exists in hdfs.

        Args:
            filepath (string) -- The filepath in Hue

        Returns:
            Bool - A boolean value which is true if file exists
    .
    """

    file_exists = hdfs.path.exists(filepath)

    return file_exists


def hdfs_file_size(filepath: str) -> int:
    """Function to check the size of a file on hdfs.

    Args:
        filepath (string) -- The filepath in Hue

    Returns:
        Int - an integer value indicating the size
        of the file in bytes
    """
    file_size = hdfs.path.getsize(filepath)

    return file_size


def check_file_exists(filepath) -> bool:
    """Checks if file exists in hdfs and is non-empty.

    If the file exists but is empty, a warning is logged.

    Args:
        filepath (str): The filepath of the file to check.

    Returns:
        bool: True if the file exists and is non-empty, False otherwise.

    Raises: a FileNotFoundError if the file doesn't exist.
    """
    output = False

    file_exists = hdfs_file_exists(filepath)

    # If the file exists on hdfs, check the size of it.
    if file_exists:
        file_size = hdfs_file_size(filepath)

    if not file_exists:
        raise FileNotFoundError(f"File {filepath} does not exist.")

    if file_exists and file_size > 0:
        output = True
        hdfs_logger.info(f"File {filepath} exists and is non-empty")

    elif file_exists and file_size == 0:
        output = False
        hdfs_logger.warning(f"File {filepath} exists on HDFS but is empty")

    return output


def hdfs_mkdir(path):
    """Function to create a directory in HDFS

    Args:
        path (string) -- The path to create
    """
    hdfs.mkdir(path)
    return None


def hdfs_open(filepath, mode):
    """Function to open a file in HDFS

    Args:
        filepath (string) -- The filepath in Hue
        mode (string) -- The mode to open the file in
    """
    file = hdfs.open(filepath, mode)
    return file


@time_logger_wrap
def hdfs_write_feather(filepath, df):
    """Function to write dataframe as feather file in HDFS"""
    with hdfs.open(filepath, "wb") as file:
        df.to_feather(file)
    # Check log written to feather
    hdfs_logger.info(f"Dataframe written to {filepath} as feather file")

    return True


@time_logger_wrap
def hdfs_read_feather(filepath):
    """Function to read feather file from HDFS"""
    with hdfs.open(filepath, "rb") as file:
        df = pd.read_feather(file)
    # Check log written to feather
    hdfs_logger.info(f"Dataframe read from {filepath} as feather file")

    return df
