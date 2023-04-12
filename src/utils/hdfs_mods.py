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


def hdfs_append(filepath: str, last_run: pd.DataFrame):
    """Function to append latest log metadata to csv in DAP

    Args:
        filepath (string): The filepath in Hue
        last_run (Dataframe): Dataframe of latest run data
    """

    # Open the file in read mode inside Hadoop context
    with hdfs.open(filepath, "r") as file:
        # Import csv file and convert to Dataframe
        df_imported_from_hdfs = pd.read_csv(file)
        # Append new data
        last_run_data = df_imported_from_hdfs.append(last_run)

    # Open the same file in write mode
    with hdfs.open(filepath, "wt") as file:
        # Write new updated dataframe to DAP context
        last_run_data.to_csv(file, index=False)

    return None
