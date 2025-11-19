"""Utility functions for the freezing module."""

from datetime import datetime
import pandas as pd
import logging

from src.utils.defence import type_defence


def _add_last_frozen_column(frozen_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Add the last_frozen column to staged data.

    Args:
        frozen_df (pd.DataFrame): The frozen data.
        config (dict[str, Any]): The pipeline configuration.

    Returns:
        pd.DataFrame: A dataframe containing the updated last_frozen column.
    """
    type_defence(frozen_df, "frozen_df", pd.DataFrame)
    type_defence(config, "run_id", dict)
    todays_date = datetime.today().strftime("%y-%m-%d")
    run_id = config["filename_items"]["run_id"]
    last_frozen = f"{todays_date}_v{str(run_id)}"
    frozen_df["last_frozen"] = last_frozen
    return frozen_df


def drop_cols(
    df: pd.DataFrame, cols: list = ["change_type", "accept_changes"]
) -> pd.DataFrame:
    """Drop columns from a dataframe.

    Args:
        df (pd.DataFrame): The dataframe to drop columns from.
        cols (list): The columns to drop.

    Returns:
        pd.DataFrame: The dataframe with the columns dropped.
    """
    for col in cols:
        if col in df.columns:
            df = df.drop(col, axis=1)
    return df


def validate_any_refinst_in_frozen(
    frozen_df: pd.DataFrame,
    df2: pd.DataFrame,
) -> bool:
    """Validate that any of the ref/inst combinations from df2 are in the frozen df.

    Args:
        frozen_df (pd.DataFrame): The frozen csv df
        df2 (pd.DataFrame): A second dataframe.

    Returns:
        bool: Whether any ref/inst combs from df2 are in frozen_df.
    """
    frozen_copy = frozen_df.copy()
    df2_copy = df2.copy()
    frozen_copy["refinst"] = frozen_copy["reference"].astype(str) + frozen_copy[
        "instance"
    ].astype(str)
    df2_copy["refinst"] = df2_copy["reference"].astype(str) + df2_copy[
        "instance"
    ].astype(str)
    result = any([x in list(frozen_copy["refinst"]) for x in list(df2_copy["refinst"])])
    return result


def validate_additions_df(
    frozen_df: pd.DataFrame,
    df: pd.DataFrame,
    FreezingLogger: logging.Logger,
) -> None:
    """Validate the additions df.

    Args:
        frozen_df (pd.DataFrame): The frozen csv df.
        df (pd.DataFrame): The dataframe of additions or deletions
        FreezingLogger (logging.Logger): The logger to log to.

    Returns:
        bool: Whether or not the additions or deletions df is valid.
    """
    # check that the ref/inst combos are not staged frozen data
    FreezingLogger.info(
        "Checking if any row in the additions or deletions df are in the frozen data..."
    )

    any_present = validate_any_refinst_in_frozen(frozen_df, df)
    if any_present:
        FreezingLogger.info(
            "Some reference/instance combinations from the additions file are "
            "present in the frozen data."
        )
        return False
    return True
