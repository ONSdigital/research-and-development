"""All genergal functions for the hdfs file system which uses PyDoop.
    These functions will need to be tested seperately, using mocking.
"""

import pydoop.hdfs as hdfs
import csv
import pandas as pd


def hdfs_csv_creator(filepath: str, columns: list):
    """Creates a csv file in DAP with user
    defined headers if it doesn't exist.
    Args:
        filename (string): Example: "name_of_file.csv"
        columns (list): Example: ["a","b","c","d"]
    """

    # Check if the file exists
    if not hdfs.path.isfile(filepath):
        # open the file in write mode inside Hadoop context
        with hdfs.open(filepath, "wt") as file:
            # Create new csv file in specified folder
            writer = csv.writer(file)
            # Add the headers to the new csv
            writer.writerow(columns)

    return None


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
    """Writes A Pandas Dataframe to csv in DAP

    Args:
        filepath (str): Filepath (Specified in config)
        data (pd.DataFrame): Data to be stored
    """
    # Open the file in write mode
    with hdfs.open(filepath, "wt") as file:
        # Write dataframe to DAP context
        data.to_csv(file, index=False)
    return None
