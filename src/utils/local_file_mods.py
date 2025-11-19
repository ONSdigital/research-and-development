"""Functions to read and write files on a local network drive

NOTE: these functions used to be name-spaced with the prefix 'local_',
but these have now been updated to 'rd_' representing "R and D".
This is so that the corresponding functions in the hdfs_file_mods.py file, which
are used when running code on hdfs, can be imported with the same name.
"""

import json
import os
import pandas as pd
import logging
import shutil
import yaml

from rdsa_utils.typing import PathLike
from rdsa_utils.helpers.python import (
    md5_sum,
    file_size,
    file_exists,
    directory_exists,
    read_header,
    create_folder,
)

# Set up logger
LocalModLogger = logging.getLogger(__name__)


def rd_read_csv(filepath: PathLike, **kwargs) -> pd.DataFrame:
    """Reads a csv file from a local Windows drive or a network drive into a
    Pandas Dataframe using Python open() function.
    If "thousands" argument is not specified, sets it to ",".
    Allows to use any additional keyword arguments of Pandas read_csv method.

    Args:
        filepath (str): Filepath
        kwargs: Optional dictionary of Pandas read_csv arguments
    Returns:
        pd.DataFrame: Dataframe created from csv
    """
    # Open the file in read mode
    with open(filepath, "r", encoding="utf-8") as file:
        # If "thousands" argument is not specified, set it to ","
        if "thousands" not in kwargs:
            kwargs["thousands"] = ","

        # Read the scv file using the path and keyword arguments
        try:
            df = pd.read_csv(file, **kwargs, low_memory=False)
        except Exception:
            LocalModLogger.error(f"Could not read specified file: {filepath}")
            if kwargs:
                LocalModLogger.info("The following arguments failed: " + str(kwargs))
            if "usecols" in kwargs:
                LocalModLogger.info("Columns not found: " + str(kwargs["usecols"]))
            raise ValueError
    return df


def rd_write_csv(filepath: str, data: pd.DataFrame):
    """Writes a Pandas Dataframe to csv on a local network drive

    Args:
        filepath (str): Filepath
        data (pd.DataFrame): Data to be stored
    """
    # Open the file in write mode
    with open(filepath, "w", newline="\n", encoding="utf-8") as file:
        # Write dataframe to the file
        data.to_csv(file, date_format="%Y-%m-%d %H:%M:%S.%f+00", index=False)


def rd_load_json(filepath: str) -> dict:
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


def rd_file_exists(filepath: str, raise_error=False) -> bool:
    """Function to check if a file exists on a local network drive
        using an rdsa_utils function

    Args:
        filepath (string): The filepath

    Returns:
        bool: A boolean value indicating whether the file exists or not
    """
    return file_exists(filepath)


def rd_file_size(filepath: str) -> int:
    """Function to check the size of a file on a local network drive
        using an rdsa_utils function

    Args:
        filepath (string): The filepath

    Returns:
        int: An integer value indicating the size of the file in bytes
    """
    return file_size(filepath)


def check_file_exists(filename: str, filepath: str) -> bool:
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

    rd_file = rd_file_exists(file_loc)

    # If the file exists locally, check the size of it.
    if rd_file:
        file_size = rd_file_size(file_loc)

    # If file does not exist locally
    if not rd_file:
        raise FileNotFoundError(f"File {filename} does not exist or is empty")

    # If file exists locally and is non-empty
    if rd_file and file_size > 0:
        output = True
        LocalModLogger.info(f"File {filename} exists and is non-empty")

    # If file exists locally but is empty
    elif rd_file and file_size == 0:
        output = False
        LocalModLogger.warning(f"File {filename} exists but is empty")

    return output


def rd_mkdir(path):
    """Creates a directory on a local network drive using an rdsa_utils function.

    Args:
        path (string) -- The path to create
    """
    create_folder(path)
    return None


def rd_write_feather(filepath, df):
    """Writes a Pandas Dataframe to a feather file on a local network drive

    Args:
        filepath (string) -- The filepath
        df (pd.DataFrame) -- The data to write
    """
    df.to_feather(filepath)
    return True


def rd_read_feather(filepath):
    """Reads a feather file from a local network drive into a Pandas DataFrame

    Args:
        filepath (str): Filepath

    Returns:
        pd.DataFrame: Dataframe created from feather file
    """
    df = pd.read_feather(filepath)
    return df


def rd_delete_file(path: str):
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


def rd_md5sum(path: str):
    """
    Get md5sum of a specific file on the local file system.

    Returns
    -------
    The md5sum of the file.
    """
    md5result = md5_sum(path)
    return md5result


def rd_stat_size(path: str):
    """
    Get the size of a file or directory in bytes on the local file system.

    Returns
    -------
    The size of the file or directory in bytes.
    """
    return os.stat(path).st_size


def rd_isdir(path: str) -> bool:
    """
    Test if directory exists on the local file system.

    Returns
    -------
    True if the directory exists. Else False.
    """
    return directory_exists(path)


def rd_isfile(path: str) -> bool:
    """
    Test if file exists on the local file system.

    Returns
    -------
    True if the file exists. Else False.
    """
    if path is None:
        return False

    return os.path.isfile(path)


def rd_read_header(path: str):
    """
    Reads the first line of a file on the local file system.

    Returns
    -------
    The first line of the file as a string.
    """
    return read_header(path)


def rd_write_string_to_file(content: bytes, path: str):
    """
    Writes a string into the specified file path on the local file system.

    Returns
    -------
    None
    """
    with open(path, "wb") as f:
        f.write(content)


def rd_copy_file(src_path: str, dst_path: str):
    """
    Copies a file from src_path to dst_path on the local file system.

    Returns: None
    """
    shutil.copy(src_path, dst_path)


def rd_move_file(src_path: str, dst_path: str):
    """Moves a file from src_path to dst_path on the local file system.

    Returns: None
    """
    shutil.move(src_path, dst_path)


def rd_list_files(path: str, ext: str = None, order: str = None):
    """
    lists all files in a directory on the local file system.

    Returns
    -------
    A list of files in the directory.
    """
    files_in_dir = [os.path.join(path, file) for file in os.listdir(path)]

    if ext:
        if not ext[0] == ".":
            # insert a dot if it's been forgotten
            ext = "." + ext
        files_in_dir = [
            file for file in files_in_dir if os.path.splitext(file)[1] == ext
        ]

    if order:
        ord_dict = {"newest": True, "oldest": False}
        files_in_dir = sorted(
            files_in_dir, key=os.path.getmtime, reverse=ord_dict[order]
        )

    return files_in_dir


def rd_search_file(dir_path, ending):
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
            # Check for ending
            if file.endswith(ending):
                target_file = str(file)

        # throw an error if no file is found
        if target_file is None:
            raise FileNotFoundError(f"No file with ending {ending} found")

    return target_file


def safeload_yaml(path: PathLike) -> dict:
    """Load a .yaml file from a path.

    Args:
        path (PathLike): The path to load the .yaml file from.

    Raises:
        FileNotFoundError: Raised if there is no file at the given path.
        TypeError: Raised if the file does not have the .yaml extension.

    Returns:
        dict: The loaded yaml file as as dictionary.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Attempted to load yaml at: {path}. File does not exist."
        )
    ext = os.path.splitext(path)[1]
    if ext != ".yaml":
        raise TypeError(f"Expected a .yaml file. Got {ext}")
    with open(path, "r") as f:
        yaml_dict = yaml.safe_load(f)
    return yaml_dict
