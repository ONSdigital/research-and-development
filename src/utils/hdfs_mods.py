"""All general functions for the hdfs file system which uses PyDoop.
    These functions will need to be tested separately, using mocking.
"""

import pandas as pd
import json
import logging
from typing import List
import subprocess
import os

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


def hdfs_write_feather(filepath, df):
    """Function to write dataframe as feather file in HDFS"""
    with hdfs.open(filepath, "wb") as file:
        df.to_feather(file)
    # Check log written to feather
    hdfs_logger.info(f"Dataframe written to {filepath} as feather file")

    return True


def _perform(
    command,
    shell: bool = False,
    str_output: bool = False,
    ignore_error: bool = False,
    full_out=False,
):
    """
    Run shell command in subprocess returning exit code or full string output.
    _perform() will build the command that will be put into HDFS.
    This will also be used for the functions below.
    Parameters
    ----------
    shell
        If true, the command will be executed through the shell.
        See subprocess.Popen() reference.
    str_output
        output exception as string
    ignore_error
    """
    process = subprocess.Popen(
        command, shell=shell, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    stdout, stderr = process.communicate()

    if str_output:
        if stderr and not ignore_error:
            raise Exception(stderr.decode("UTF-8").strip("\n"))
        if full_out:
            return stdout
        return stdout.decode("UTF-8").strip("\n")

    return process.returncode == 0


def hdfs_delete_file(path: str):
    """
    Delete a file. Uses 'hadoop fs -rm'.

    Returns
    -------
    True for successfully completed operation. Else False.
    """
    command = ["hadoop", "fs", "-rm", path]
    return _perform(command)


def hdfs_md5sum(path: str):
    """
    Get md5sum of a specific file on HDFS.
    """
    return _perform(
        f"hadoop fs -cat {path} | md5sum",
        shell=True,
        str_output=True,
        ignore_error=True,
    ).split(" ")[0]


def hdfs_stat_size(path: str):
    """
    Runs stat command on a file or directory to get the size in bytes.
    """
    command = ["hadoop", "fs", "-du", "-s", path]
    return _perform(command, str_output=True).split(" ")[0]


def hdfs_isdir(path: str) -> bool:
    """
    Test if directory exists. Uses 'hadoop fs -test -d'.

    Returns
    -------
    True for successfully completed operation. Else False.
    """
    command = ["hadoop", "fs", "-test", "-d", path]
    return _perform(command)


def hdfs_isfile(path: str) -> bool:
    """
    Test if file exists. Uses 'hadoop fs -test -f.

    Returns
    -------
    True for successfully completed operation. Else False.

    Note
    ----
    If checking that directory with partitioned files (i.e. csv, parquet)
    exists this will return false use isdir instead.
    """
    command = ["hadoop", "fs", "-test", "-f", path]
    return _perform(command)


def hdfs_read_header(path: str):
    """
    Reads the first line of a file on HDFS
    """
    return _perform(
        f"hadoop fs -cat {path} | head -1",
        shell=True,
        str_output=True,
        ignore_error=True,
    )


def hdfs_write_string_to_file(content: bytes, path: str):
    """
    Writes a string into the specified file path
    """
    _write_string_to_file = subprocess.Popen(
        f"hadoop fs -put - {path}", stdin=subprocess.PIPE, shell=True
    )
    return _write_string_to_file.communicate(content)


def hdfs_copy_file(src_path: str, dst_path: str):
    """
    Copy a file from one location to another. Uses 'hadoop fs -cp'.
    """
    command = ["hadoop", "fs", "-cp", src_path, dst_path]
    return _perform(command)


def hdfs_move_file(src_path: str, dst_path: str):
    """
    Move a file from one location to another. Uses 'hadoop fs -mv'.
    """
    command = ["hadoop", "fs", "-mv", src_path, dst_path]
    return _perform(command)


def hdfs_list_files(path: str, ext: str = None):
    """
    List files in a directory. Uses 'hadoop fs -ls'.
    """
    # Forming the command line command and executing
    command = ["hadoop", "fs", "-ls", path]
    files_as_str = _perform(command, str_output=True)

    # Breaking up the returned string, and stripping down to just paths of files
    file_paths = [line.split()[-1] for line in files_as_str.strip().split("\n")[1:]]

    # Getting just the filename (including ext) alone
    file_names_in_dir = [os.path.basename(path) for path in file_paths]

    # Filtering the files to just those with the required extension
    if ext:
        ext = f".{ext}"
        file_names_in_dir = [
            file for file in file_names_in_dir if os.path.splitext(file)[1] == ext
        ]

    return file_names_in_dir


def hdfs_search_file(dir_path, ending):
    """Find a file in a directory with a specific ending using grep and hadoop fs -ls.

    Args:
        dir_path (_type_): _description_
        ending (_type_): _description_
    """
    target_file = _perform(
        f"hadoop fs -ls {dir_path} | grep {ending}",
        shell=True,
        str_output=True,
        ignore_error=True,
    )

    # Handle case where file does not exist
    if not target_file:
        raise FileNotFoundError(
            f"File with ending {ending} does not exist in {dir_path}"
        )

    # Return file path + name
    target_file = target_file.split()[-1]

    return target_file
