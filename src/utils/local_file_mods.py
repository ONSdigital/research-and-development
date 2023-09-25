"""Functions to read and write files on a local network drive"""

import json
import os
import pandas as pd
import logging
from typing import List
import hashlib
import shutil


# Set up logger
lfmod_logger = logging.getLogger(__name__)


def read_local_csv(filepath: str, cols: List[str] = None) -> pd.DataFrame:
    """Reads a csv from a local network drive into a Pandas DataFrame
    Args:
        filepath (str): Filepath
        cols (List[str]): Optional list of columns to be read in
    Returns:
        pd.DataFrame: Dataframe created from csv
    """
    # Open the file in read mode
    with open(filepath, "r") as file:
        # Import csv file and convert to Dataframe
        if not cols:
            df = pd.read_csv(file)
        else:
            try:
                df = pd.read_csv(file, usecols=cols)
            except Exception:
                lfmod_logger.error(f"Could not find specified columns in {filepath}")
                lfmod_logger.info("Columns specified: " + str(cols))
                raise ValueError
    return df


def write_local_csv(filepath: str, data: pd.DataFrame):
    """Writes a Pandas Dataframe to csv on a local network drive

    Args:
        filepath (str): Filepath
        data (pd.DataFrame): Data to be stored
    """
    # Open the file in write mode
    with open(filepath, "w", newline="\n", encoding="utf-8") as file:
        # Write dataframe to the file
        data.to_csv(file, date_format="%Y-%m-%d %H:%M:%S.%f+00", index=False)


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


def local_delete_file(path: str):
    """
    Delete a file on the local file system.

    Returns
    -------
    True for successfully completed operation. Else False.
    """
    try:
        os.remove(path)
        return True
    except OSError:
        return False


def local_md5sum(path: str):
    """
    Get md5sum of a specific file on the local file system.

    Returns
    -------
    The md5sum of the file.
    """
    with open(path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()


def local_stat_size(path: str):
    """
    Get the size of a file or directory in bytes on the local file system.

    Returns
    -------
    The size of the file or directory in bytes.
    """
    return os.stat(path).st_size


def local_isdir(path: str) -> bool:
    """
    Test if directory exists on the local file system.

    Returns
    -------
    True if the directory exists. Else False.
    """
    return os.path.isdir(path)


def local_isfile(path: str) -> bool:
    """
    Test if file exists on the local file system.

    Returns
    -------
    True if the file exists. Else False.
    """
    return os.path.isfile(path)


def local_read_header(path: str):
    """
    Reads the first line of a file on the local file system.

    Returns
    -------
    The first line of the file as a string.
    """
    with open(path, "r") as f:
        return f.readline()


def local_write_string_to_file(content: bytes, path: str):
    """
    Writes a string into the specified file path on the local file system.

    Returns
    -------
    None
    """
    with open(path, "wb") as f:
        f.write(content)


def local_copy_file(src_path: str, dst_path: str):
    """
    Copies a file from src_path to dst_path on the local file system.

    Returns: None
    """
    shutil.copy(src_path, dst_path)


def local_move_file(src_path: str, dst_path: str):
    """Moves a file from src_path to dst_path on the local file system.

    Returns: None
    """
    shutil.move(src_path, dst_path)


def local_list_files(path: str, ext: str = None):
    """
    Lists all files in a directory on the local file system.

    Returns
    -------
    A list of files in the directory.
    """
    files_in_dir = os.listdir(path)

    if ext:
        files_in_dir = [
            file for file in files_in_dir if os.path.splitext(file)[1] == ext
        ]

    return files_in_dir


def local_search_file(dir_path, ending):
    """Search for a file with a particular suffix in a directory on the local
        file system.

    Args:
        path (_type_): _description_
        ending (_type_): _description_

    Returns:
        str: The path of the target file
    """
    for _, __, files in os.walk(dir_path):
        for file in files:

            # change the extension from '.mp3' to
            # the one of your choice.
            if file.endswith("ending"):
                target_file = str(file)

    return target_file
