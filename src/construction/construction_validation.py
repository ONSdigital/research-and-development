"""Utilities to validate the construction process."""
import logging
from typing import Union, Tuple

import pandas as pd

from src.utils.defence import type_defence


def check_for_duplicates(
    df: pd.DataFrame, columns: Union[list, str], logger: logging.Logger = None
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
    validate_dupes: bool = False,
    logger: logging.Logger = None,
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
    type_defence(validate_dupes, "validate_dupes", bool)
    type_defence(logger, "logger", (logging.Logger, type(None)))
    if logger:
        logger.info("Merging dataframes for construction...")
    merged = pd.concat([df1, df2]).reset_index(drop=True)
    if validate_dupes:
        if logger:
            logger.info("Merged dataframes are being checked for duplicates...")
        check_for_duplicates(merged, ["reference", "instance"], logger)
    return merged


def validate_columns_not_empty(
    df: pd.DataFrame,
    columns: Union[str, list],
    logger: logging.Logger = None,
    _raise: bool = True,
) -> None:
    """Validate that all columns in a set are not empty.

    Args:
        df (pd.DataFrame): The dataframe to check.
        columns (Union[str, list]): The columns to check.
        logger (logging.Logger, optional): A logger. Defaults to None.
        _raise (bool, optional): Whether to raise an error if all columns are
            empty. Defaults to True.

    Raises:
        IndexError: Raised when a column is passed that isn't present in the df.
        ValueError: Raised when all columns in the subset are empty for a row.

    Returns:
        None
    """
    # defences
    type_defence(df, "df", (pd.DataFrame))
    type_defence(columns, "columns", (str, list))
    type_defence(logger, "logger", (logging.Logger, type(None)))
    type_defence(_raise, "_raise", bool)
    # standardize 'columns'
    if isinstance(columns, str):
        columns = [columns]
    # check passed columns are in the dataframe
    for column in columns:
        if column not in df.columns:
            raise IndexError(f"Column {column} is not in the passed dataframe.")
    # validate whether there are missing values in all columns of a row
    if len(df[columns].dropna(axis=0, how="all")) != len(df):
        if _raise:
            raise ValueError(f"Columns {columns} are both empty.")
        else:
            logger.info(f"Columns {columns} are both empty.")
    # write confirmation to log
    if logger:
        logger.info(f"All rows have a valid value for one of columns {columns}.")
    return None


def validate_short_to_long(df: pd.DataFrame, logger: logging.Logger = None) -> None:
    """Validate the short_to_long construction records.

    Args:
        df (pd.DataFrame): The dataframe to validate.
        logger (logging.Logger, optional): The logger to log to.

    Raises:
        ValueError: Raised when there are no records of instance=0 for a
            reference/period.
    """
    # defences
    type_defence(df, "df", (pd.DataFrame))
    type_defence(logger, "logger", (logging.Logger, type(None)))
    # validates that all key columns have values
    validate_columns_not_empty(
        df=df, columns=["reference", "instance", "period"], logger=logger
    )
    # refine dataframe to required data
    df = df[df.construction_type == "short_to_long"]
    df = df[["reference", "instance", "period"]]
    # validate that all short_to_long constructions have instance=0
    min_df = df.groupby(["reference", "period"]).agg("min").reset_index()
    if len(min_df.instance) > 1 and min_df.instance[0] != 0:
        raise ValueError(
            "Short to long construction requires at least record where instance=0 for"
            "each reference/period."
        )
    logger.info("All short_to_long construction rows have valid instances.")
    return None


def _references_in_snapshot(
    df: pd.DataFrame, snapshot_refs: list, logger: logging.Logger = None
) -> Tuple[bool, list]:
    """Determine if the references in a df are in a snapshot.

    Args:
        df (pd.DataFrame): The construction df (With references).
        snapshot_refs (list): A list of the references in the snapshot df.
        logger (logging.Logger): The logger to log to.

    Returns:
        Tuple[bool, list]: Whether or not all references are in the snapshot,
            a list of all references missing from the snapshot.
    """
    type_defence(df, "df", pd.DataFrame)
    type_defence(snapshot_refs, "snapshot_refs", list)
    type_defence(logger, "logger", (logging.Logger, type(None)))
    # add a new column to the dataframe indicating if the ref is present
    valid_df = df.join(
        df[["reference"]].isin(snapshot_refs), how="left", rsuffix="_valid"
    )
    # obtain a df of invalid references (not in snpashot)
    invalid_refs = valid_df[valid_df.reference_valid == False]
    if len(invalid_refs) > 0:
        inv = invalid_refs["reference"].unique()
        if logger:
            logger.info(
                f"Reference(s) in construction file not in snapshot: {inv}",
            )
        return (False, list(inv))
    if logger:
        logger.info("All passed references from construction file in snapshot")
    return (True, [])


def validate_construction_references(
    df: pd.DataFrame, snapshot_df: pd.DataFrame, logger: logging.Logger = None
) -> None:
    """Validate the construction references for both new and non-new constructions.

    Args:
        df (pd.DataFrame): The construction dataframe.
        snapshot_df (pd.DataFrame): The snapshot dataframe.
        logger (logging.Logger, optional): The logger. Defaults to None.

    Raises:
        ValueError: Raised if non new construction refs aren't present.
        ValueError: Raised if new construction refs are already present.

    Returns:
        None
    """
    type_defence(df, "df", pd.DataFrame)
    type_defence(snapshot_df, "snapshot_df", pd.DataFrame)
    type_defence(logger, "logger", (logging.Logger, type(None)))
    snapshot_refs = list(snapshot_df.reference.unique())
    new_constructions = df[df.construction_type == "new"]
    reg_constructions = df[df.construction_type != "new"]
    # non new constructions
    refs_valid, refs = _references_in_snapshot(
        df=reg_constructions, snapshot_refs=snapshot_refs, logger=logger
    )
    if not refs_valid:
        raise ValueError(
            "References in construction file are not included in the original "
            f"data: {refs}"
        )
    if logger:
        logger.info(
            "References not marked as new constructions are all in the original dataset"
        )
    # new constructions
    if new_constructions.empty:
        return None
    refs_valid, refs = _references_in_snapshot(
        df=new_constructions, snapshot_refs=snapshot_refs, logger=logger
    )
    if refs_valid:
        raise ValueError(
            "References in the construction file labelled 'new' are already in"
            " the dataset."
        )
    if logger:
        logger.info("All references marked as 'new' are validated as new.")
    return None
