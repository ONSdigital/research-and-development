"""All genergal functions for the hdfs file system which uses PyDoop.
    These functions will need to be tested seperately, using mocking.
"""

import pydoop.hdfs as hdfs
import pandas as pd


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
