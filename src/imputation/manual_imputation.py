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
        ManualImputationLogger.info(
            f"A column with name {column_name} already exists in the DataFrame."
            "A new trim column will not be added"
        )
        return df

    df[column_name] = trim_bool

    return df


# check if any files are in imputation/manual_trimming folder and check if 
# load_manual_imputation is True- if so load the file and any records which are marked 
# True in the manual_trim column will be excluded from the imputation process and will 
# be output as is. They will be marked as 'manual_trim' in the imp_marker column
def merge_manual_imputation(
    df: pd.DataFrame,
    manual_trim_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Loads a manual trimming file if it exists and adds a manual_trim column 
    to the DataFrame.

    Args:
        config (Dict[str, Any]): The configuration dictionary.
        df (pd.DataFrame): The dataframe to be imputed.
        isfile_func (callable): The function to use to check if the file exists.
    Returns:
        pd.DataFrame: The DataFrame with the manual_trim column added.
    """
    if manual_trim_df is not None:
        # An empty df will be initialised if there's no man trim file
        if "manual_trim" in df.columns:
            df = df.drop(columns=["manual_trim"])

        df = df.merge(manual_trim_df, on=["reference", "instance"], how="left")
        df["manual_trim"] = df["manual_trim"].fillna(False).astype(bool)
    else:
        df = add_trim_column(df)
    return df
