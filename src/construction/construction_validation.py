"""Utilities to validate the construction process."""
import logging
from typing import Union

import pandas as pd

from src.utils.defence import type_defence

def check_for_duplicates(
        df: pd.DataFrame, 
        columns: Union[list, str], 
        logger: logging.Logger = None
    ) -> None:
    """Check a dataframe for duplicate values across multiple columns.

    Args:
        df (pd.DataFrame): The dataframe to check for duplicates
        columns (Union[list, str]): The columns to check duplicates across.
        logger (logging.Logger, optional): The logger to log to. Defaults to None.
    """
    # defence checks
    type_defence(df, "df", (pd.DataFrame))
    type_defence(columns, "columns", (str, list))
    type_defence(logger, "logger", (logging.Logger, type(None)))
    if isinstance(columns, str):
        columns = [columns]
    for column in columns:
        if column not in df.columns:
            raise IndexError(f"Column {column} is not in the passed dataframe.")
    # Check for duplicates
    if logger:
        logger.info("Checking construction dataframe for duplicates...")
    refined_df = df[columns]
    if not refined_df[refined_df.duplicated()].empty:
        raise ValueError(
            f"Duplicates found in construction file.\n{refined_df.duplicated()}"
            "\nAborting pipeline."
        )
    if logger:
        logger.info("No duplicates found in construction files. Continuing...")
    return None


def concat_construction_dfs(
        df1: pd.DataFrame,
        df2: pd.DataFrame, 
        validate: bool = False,
        logger: logging.Logger = None
    ) -> pd.DataFrame:
    """Merge the construction and postcode construction dataframes into one.

    Args:
        df1 (pd.DataFrame): The first dataframe (construction).
        df2 (pd.DataFrame): The second dataframe (postcode construction).
        validate (bool, optional): Whether or not to check for duplicate 
            instance+reference in the merged dataframes. Defaults to False.
        logger (logging.Logger, optional): A logger to log to. Defaults to None.

    Returns:
        pd.DataFrame: The merged dataframe.
    """
    type_defence(df1, "df1", pd.DataFrame)
    type_defence(df2, "df1", pd.DataFrame)
    type_defence(validate, "validate", bool)
    type_defence(logger, "logger", (logging.Logger, type(None)))
    if logger:
        logger.info("Merging dataframes for construction...")
    merged = pd.concat([df1, df2]).reset_index(drop=True)
    if validate:
        if logger:
            logger.info("Merged dataframes are being checked for duplicates...")
        check_for_duplicates(merged, ["reference", "instance"], logger)
    return merged