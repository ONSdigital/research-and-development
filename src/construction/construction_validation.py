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
