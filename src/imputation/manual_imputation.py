import os
import glob
from typing import Dict, Any
import pandas as pd
import logging

from src.utils.wrappers import validate_dataframe_not_empty

ManualImputationLogger = logging.getLogger(__name__)

@validate_dataframe_not_empty
def add_trim_column(
    df: pd.DataFrame, column_name: str = "manual_trim", 
    trim_bool: bool = False
) -> pd.DataFrame:
    """
    Adds a new column to a DataFrame with a default value.

    Args:
        df (pd.DataFrame): The DataFrame to add the new column to.
        column_name (str, optional): The name of the new column. Defaults to 'manual_trim'.
        value (bool, optional): The default value for the new column. Defaults to False.

    Returns:
        pd.DataFrame: The DataFrame with the new column added.

    Raises:
        ValueError: If the DataFrame is empty or the column already exists in the DataFrame.
    """
    if column_name in df.columns:
        raise ValueError(
            f"A column with name {column_name} already exists in the DataFrame."
        )

    df[column_name] = trim_bool
    
    return df


def get_latest_csv(directory: str) -> str:
    """
    Gets the latest CSV file in a directory.

    Args:
        directory (str): The directory to search for CSV files.

    Returns:
        str: The path of the latest CSV file, or an empty string if no CSV files are found.
    """
    list_of_files = glob.glob(
        os.path.join(directory, "*.csv")
    )  # get list of all csv files
    if not list_of_files:  # if list is empty, return empty string
        return None
    latest_file = max(list_of_files, key=os.path.getctime)  # get the latest file

    return latest_file


# check if any files are in imputation/manual_trimming folder and check if load_manual_imputation is True
# if so load the file and any records which are marked True in the manual_trim column will be
# excluded from the imputation process and will be output as is. They will be marked as 'manual_trim' in the imp_marker column
def load_manual_imputation(
    df: pd.DataFrame,
    config: Dict[str, Any],
    isfile_func: callable,
    imp_path: str
) -> pd.DataFrame:
    """
    Loads a manual trimming file if it exists and adds a manual_trim column to the DataFrame.

    Args:
        config (Dict[str, Any]): The configuration dictionary.
        df (pd.DataFrame): The dataframe to be imputed.
        isfile_func (callable): The function to use to check if the file exists.
    Returns:
        pd.DataFrame: The DataFrame with the manual_trim column added.
    """

    new_man_trim_file = get_latest_csv(f"{imp_path}/manual_trimming/")

    if config["global"]["load_manual_imputation"] and isfile_func(new_man_trim_file):
        manual_trim_df = pd.read_csv(new_man_trim_file)
        df = df.merge(manual_trim_df, on=["reference", "instance"], how="left")
        df = df.drop(columns=["manual_trim_x"])
        df = df.rename(columns={"manual_trim_y": "manual_trim"})
        df["manual_trim"] = df["manual_trim"].fillna(False)
    else:
        df = add_trim_column(df)
    return df
