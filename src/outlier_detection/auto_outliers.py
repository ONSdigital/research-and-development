"""Apply outlier detection to the dataset."""
import logging
import pandas as pd
from typing import List
import math


AutoOutlierLogger = logging.getLogger(__name__)


def validate_config(upper_clip: float, lower_clip: float, flag_value_cols: List[str]):
    """Validate the outlier config settings.

    Args:
        upper_clip (float): The percentage for upper clipping as float
        lower_clip (float): The percentage for lower clipping as float
        flag_value_cols (list[str]): List of names of columns to flag.

    Raises:
        ImportError if upper_clip or lower_clip have invalid values
    """
    AutoOutlierLogger.info(f"Upper clip: {upper_clip}, Lower clip: {lower_clip}")
    AutoOutlierLogger.info(f"The columns to be flagged for outliers: {flag_value_cols}")

    if (upper_clip == 0) and (lower_clip == 0):
        AutoOutlierLogger.warning(
            "upper_clip and lower_clip both zero: All auto_outlier flags will be False"
        )
    elif (upper_clip < 0) | (lower_clip < 0):
        AutoOutlierLogger.error("upper_clip and lower_clip cannot be negative")
        raise ImportError
    elif upper_clip + lower_clip >= 1.0:
        AutoOutlierLogger.error("upper_clip and lower_clip must sum to < 1")
        raise ImportError

    if not isinstance(flag_value_cols, list):
        AutoOutlierLogger.error(
            "In config, flag_value_cols must be specified as a list."
        )
        raise ImportError


def filter_valid(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    """Filter for valid responses in a given value column.

    Outliers are only computed from valid, positive responses
    of PRN sampled data (selectiontype = P).
    Short to long form conversion has been done, so only use instance 0.
    Valid responses have statuses of Clear or Clear - overridden

    Args:
        df (pd.DataFrame): The dataframe of responses to be filtered.
        value_col (str): The name of the value column.

    Returns:
        pd.DataFrame: The filtered dataframe
    """
    sample_cond = df.selectiontype == "P"
    status_cond = df["status"].isin(["Clear", "Clear - overridden"])
    pos_cond = df[value_col] > 0
    ins_cond = df["instance"] == 0

    filtered_df = df[sample_cond & status_cond & pos_cond & ins_cond].copy()

    if filtered_df.empty:
        AutoOutlierLogger.error(
            f"column {value_col} has no valid returns for outliers."
            "This column should not be considered for outliers."
        )
        raise ValueError

    return filtered_df


def flag_outliers(
    df: pd.DataFrame, upper_clip: float, lower_clip: float, value_col: str
) -> pd.DataFrame:
    """Create Boolean column to flag outliers in a given column.

    Args:
        df (pd.DataFrame): The dataframe used for finding outliers
        upper_clip (float): The percentage for upper clipping as float
        lower_clip (float): The percentage for lower clipping as float
        value_col (str): The name of the column outliers are calculated for
    Returns:
        pd.DataFrame: The same dataframe with a new boolean column indicating
                        whether the column 'value_col' is an outlier.
    """
    # Define groups for outliers: cell number and period
    groupby_cols = ["period", "cellnumber"]

    # Define RU reference column
    ruref_col = "reference"

    # Filter for valid sampled data and positive values in the value column
    filtered_df = filter_valid(df, value_col)

    # Add group count - how many RU refs there are in a cell, perod
    filtered_df["group_count"] = filtered_df.groupby(groupby_cols)[value_col].transform(
        "count"
    )  # noqa

    # Rank margins
    filtered_df["high"] = filtered_df["group_count"] * upper_clip
    filtered_df["high_rounded"] = filtered_df.apply(
        lambda row: normal_round(row["high"]), axis=1
    )
    filtered_df["upper_band"] = filtered_df["group_count"] - filtered_df["high_rounded"]

    filtered_df["low"] = filtered_df["group_count"] * lower_clip
    filtered_df["lower_band"] = filtered_df.apply(
        lambda row: normal_round(row["low"]), axis=1
    )

    # Ranks of RU refs in each group, depending on their value
    filtered_df["group_rank"] = filtered_df.groupby(groupby_cols)[value_col].rank(
        method="first", ascending=True
    )

    # Outlier conditions
    outlier_cond = (filtered_df["group_rank"] > filtered_df["upper_band"]) | (
        filtered_df["group_rank"] <= filtered_df["lower_band"]
    )

    # Create outlier flag
    filtered_df[f"{value_col}_outlier_flag"] = outlier_cond

    # Select columns that would be joined back to main df
    cols_sel = groupby_cols + [ruref_col, f"{value_col}_outlier_flag"]
    filtered_df = filtered_df[cols_sel]

    # merge back to the original df
    df = df.merge(filtered_df, how="left", on=groupby_cols + [ruref_col])
    df[f"{value_col}_outlier_flag"] = df[f"{value_col}_outlier_flag"].fillna(False)

    return df


def decide_outliers(df: pd.DataFrame, flag_value_cols: List[str]) -> pd.DataFrame:
    """Determine whether a reference should be treated as an outlier.

    If any of the colunns in the list flag_value_cols has been flagged as an
    outlier, then the whole reference (ie, row) will be treated as an
    outlier with a flag of 'True' in the auto_outlier column.

    Args:
        df (pd.DataFrame): The main dataset where outliers are to be calculated
        flag_value_cols: (List[str]): The names of the columns to flag
        for outliers

    Returns:
        df (pd.DataFrame): The full dataset with flag column indicating whether
                            a reference should be considered an outlier.
    """
    flag_cols = [f"{col}_outlier_flag" for col in flag_value_cols]
    df["auto_outlier"] = df[flag_cols].sum(axis=1) > 0
    return df


def log_outlier_info(df: pd.DataFrame, value_col: str):
    """Log the number of outliers flagged.

    Also calculate the total number of 'valid' entries in the specified
    value_col column being flagged.

    Args:
        df (pd.DataFrame): The dataframe used for finding outliers
        value_col (str): The name of the col outliers are calculated for
    """
    filtered_df = filter_valid(df, value_col)

    flag_col = f"{value_col}_outlier_flag"

    num_flagged = filtered_df[filtered_df[flag_col]][flag_col].count()
    tot_nonzero = filtered_df[value_col].count()

    msg = (
        f"{num_flagged} outliers were detected out of a total of "
        f"{tot_nonzero} valid entries in column {value_col}"
    )
    AutoOutlierLogger.info(msg)


def run_auto_flagging(
    df: pd.DataFrame, upper_clip: float, lower_clip: float, flag_value_cols: List[str]
) -> pd.DataFrame:
    """Flag outliers based on clipping values specified in the config.

    For each of the 'flag_cols' columns specified in the developers config, a
    boolean flag is created for the highest values in the in the column,
    based on the upper_clip percentage, and the lowest non-zero values,
    based on the lower_clip percengage (usually the lower clip will be zero,
    so not effective.) Zero or null values are not included in the
    flagging process.

    A master outlier flag, 'auto_outlier', is created with a value True if
    any one of the other outlier flags is True.

    Notes:
        - Outliering is only done for randomly sampled (rather than 'census')
          data, so flags are only created where column 'seclectiontype' = 'P'.

    Args:
        df (pd.DataFrame): The main dataset where outliers are to be calculated
        upper_clip (float): The percentage for upper clipping
        lower_clip (float): The percentage for lower clipping
        flag_value_cols: (List[str]): The names of the columns to flag
        for outliers

    Returns:
        df (pd.DataFrame): The full dataset with flag columns indicating
        outliers.
    """
    AutoOutlierLogger.info("Starting outlier detection...")

    # Validate the outlier configuration settings
    validate_config(upper_clip, lower_clip, flag_value_cols)

    # loop through all columns to be flagged for outliers
    for value_col in flag_value_cols:
        # Call function to add a flag for auto outliers in column value_col
        df = flag_outliers(df, upper_clip, lower_clip, value_col)

        # Log infomation on the number of outliers in column value_col
        log_outlier_info(df, value_col)

    # create 'master' outlier column- which is True if any of the other
    # flags is True
    df = decide_outliers(df, flag_value_cols)

    # log the number of True flags in the master outlier flag column
    num_flagged = df[df["auto_outlier"]]["auto_outlier"].count()

    msg = f"Outlier flags have been createdfor {num_flagged} records in total."
    AutoOutlierLogger.info(msg)

    AutoOutlierLogger.info("Finishing automatic outlier detection.")

    return df


def apply_short_form_filters(
    df: pd.DataFrame,
    form_type_no: str = "0006",
    sel_type: str = "P",
):
    """Apply a filter for only short forms with selection type 'P'."""
    # Filter to the correct form type
    form_cond = df["formtype"] == form_type_no
    sel_cond = df["selectiontype"] == sel_type
    ins_cond = df["instance"] == 0
    filtered_df = df[form_cond & sel_cond & ins_cond]

    return filtered_df


def normal_round(x: float) -> int:
    """Simple rounding, so that 0.5 rounds to 1,
    as opposed to default banking rounding that rounds halves
    to nearest even integers.

    Args:
        x (float): Fractional number to be rounded
    Returns:
        int: Rounded value
    """
    f = math.floor(x)
    if x - f < 0.5:
        return f
    else:
        return f + 1
