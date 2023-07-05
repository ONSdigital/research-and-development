"""Functions to read and write files on a local network drive"""

import json
import os
import pandas as pd
import logging


# Set up logger 
lfmod_logger = logging.getLogger(__name__)

def read_local_csv(filepath: str) -> pd.DataFrame:
    """Reads a csv from a local network drive into a Pandas DataFrame
    Args:
        filepath (str): Filepath

    Returns:
        pd.DataFrame: Dataframe created from csv
    """
    # Open the file in read mode
    with open(filepath, "r") as file:
        # Import csv file and convert to Dataframe
        df = pd.read_csv(file)

    return df


def write_local_csv(filepath: str, data: pd.DataFrame):
    """Writes a Pandas Dataframe to csv on a local network drive

    Args:
        filepath (str): Filepath
        data (pd.DataFrame): Data to be stored
    """
    # Open the file in write mode
    with open(filepath, "w", newline='\n') as file:
        # Write dataframe to the file
        data.to_csv(file, index=False)


def load_local_json(filepath: str) -> dict:
    """Function to load JSON data from a file on a local network drive
    Args:
        filepath (string): The filepath

    Returns:
        dict: JSON data
    """
    # Open the file in read mode
    with open(filepath, "r") as file:
        # Load JSON data from the file
        data = json.load(file)

    return data


def local_file_exists(filepath: str) -> bool:
    """Function to check if a file exists on a local network drive

    Args:
        filepath (string): The filepath

    Returns:
        bool: A boolean value indicating whether the file exists or not
    """
    file_exists = os.path.exists(filepath)

    return file_exists


def local_file_size(filepath: str) -> int:
    """Function to check the size of a file on a local network drive

    Args:
        filepath (string): The filepath

    Returns:
        int: An integer value indicating the size of the file in bytes
    """
    file_size = os.path.getsize(filepath)

    return file_size


def check_file_exists(filename: str, filepath: str = "./data/raw/") -> bool:
    """Checks if file exists on a local network drive and is non-empty.
    Raises a FileNotFoundError if the file doesn't exist.

    Keyword Arguments:
        filename (str): Name of file to check
        filepath (str): Relative path to file (default: "./data/raw/")

    Returns:
        bool: True if the file exists and is non-empty, False otherwise.
    """
    output = False

    file_loc = os.path.join(filepath, filename)

    local_file = local_file_exists(file_loc)

    # If the file exists locally, check the size of it.
    if local_file:
        file_size = local_file_size(file_loc)

    # If file does not exist locally
    if not local_file:
        raise FileNotFoundError(f"File {filename} does not exist or is empty")

    # If file exists locally and is non-empty
    if local_file and file_size > 0:
        output = True
        lfmod_logger.info(f"File {filename} exists and is non-empty")

    # If file exists locally but is empty
    elif local_file and file_size == 0:
        output = False
        lfmod_logger.warning(f"File {filename} exists but is empty")

    return output

def local_mkdir(path):
    """Creates a directory on a local network drive
    
    Args:
        path (string) -- The path to create
    """
    os.mkdir(path)
    return None


def local_open(filepath, mode):
    """Opens a file on a local network drive
    
    Args:
        filepath (string) -- The filepath
        mode (string) -- The mode to open the file in
    """
    file = open(filepath, mode)
    return file


def local_file_write_feather(filepath, df):
    """Writes a Pandas Dataframe to a feather file on a local network drive
    
    Args:
        filepath (string) -- The filepath
        df (pd.DataFrame) -- The data to write
    """
    df.to_feather(filepath)
    return True