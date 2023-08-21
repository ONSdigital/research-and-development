"""Apply outlier detection to the dataset."""
import logging
import pandas as pd
from typing import List


AutoOutlierLogger = logging.getLogger(__name__)

# Set defaults
LOWER_BAND_DEFAULT = 0
UPPER_BAND_DEFAULT = 1


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
            "upper_clip and lower_clip both zero: "
            "All auto_outlier flags will be False"
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
    lower_band = LOWER_BAND_DEFAULT + lower_clip
    upper_band = UPPER_BAND_DEFAULT - upper_clip

    # Note: selectiontype=='P' doesn't exist in anonymised data!
    filter_cond = (df[value_col] > 0) & (df.selectiontype == "P")
    filtered_df = df[filter_cond]
    if filtered_df.empty:
        AutoOutlierLogger.error(
            "This data does not contain value 'P' in "
            "column 'selectiontype'. \n Note that outliering cannot be "
            "performed on the current anonomysed spp snapshot data."
        )
        raise ValueError

    groupby_cols = ["period", "cellnumber"]

    quantiles_up = (
        filtered_df[groupby_cols + [value_col]]
        .groupby(groupby_cols)
        .quantile([upper_band], interpolation="nearest")
        .reset_index()
    )
    quantiles_up = quantiles_up.rename(columns={value_col: "upper_band"})[
        groupby_cols + ["upper_band"]
    ]

    quantiles_low = (
        filtered_df[groupby_cols + [value_col]]
        .groupby(groupby_cols)
        .quantile([lower_band], interpolation="nearest")
        .reset_index()
    )
    quantiles_low = quantiles_low.rename(columns={value_col: "lower_band"})[
        groupby_cols + ["lower_band"]
    ]

    df = df.merge(quantiles_up, on=groupby_cols, how="left").merge(
        quantiles_low, on=groupby_cols, how="left"
    )

    outlier_cond = (df[value_col] > df.upper_band) | (df[value_col] < df.lower_band)

    # create boolean auto_outlier col based on conditional logic
    df[f"{value_col}_outlier_flag"] = outlier_cond & filter_cond
    df = df.drop(["upper_band", "lower_band"], axis=1)
    return df


def decide_outliers(df: pd.DataFrame, flag_value_cols: List[str]) -> pd.DataFrame:
    """Determine whether a reference should be treated as an outlier.

    If any of the colunns in the list flag_value_cols has been flagged as an
    outlier, then the whole reference (ie, row) will be treated as an
    outlier with a flag of 'True' in the auto_outlier column.

    Args:
        df (pd.DataFrame): The main dataset where outliers are to be calculated
        flag_value_cols: (List[str]): The names of the columns to flag for outliers

    Returns:
        df (pd.DataFrame): The full dataset with flag column indicating whether
                            a reference should be considered an outlier.
    """
    flag_cols = [f"{col}_outlier_flag" for col in flag_value_cols]
    df["auto_outlier"] = df[flag_cols].sum(axis=1) > 0
    return df


def log_outlier_info(df: pd.DataFrame, value_col: str):
    """Log the number of outliers flagged.

    Also calculate the total number of non-zero entries in the specified
    value_col column being flagged.

    Args:
        df (pd.DataFrame): The dataframe used for finding outliers
        value_col (str): The name of the col outliers are calculated for
    """
    flag_col = f"{value_col}_outlier_flag"
    num_flagged = df[df[flag_col]][flag_col].count()

    tot_nonzero = df[df[value_col] > 0][value_col].count()

    msg = (
        f"{num_flagged} outliers were detected out of a total of "
        f"{tot_nonzero} non-zero entries in column {value_col}"
    )
    AutoOutlierLogger.info(msg)


def run_auto_flagging(
    df: pd.DataFrame, upper_clip: float, lower_clip: float, flag_value_cols: List[str]
) -> pd.DataFrame:
    """Flag outliers based on clipping values specified in the config.

    For each of the 'flag_cols' columns specified in the developers config, a
    boolean flag is created for the highest values in the in the column, based on
    the upper_clip percentage, and the lowest non-zero values, based on the
    lower_clip percengage (usually the lower clip will be zero, so not effective.)
    Zero or null values are not included in the flagging process.

    A master outlier flag, 'auto_outlier', is created with a value True if any one
    of the other outlier flags is True.

    Notes:
        - Outliering is only done for randomly sampled (rather than 'census')
          data, so flags are only created where column 'seclectiontype' = 'P'.

    Args:
        df (pd.DataFrame): The main dataset where outliers are to be calculated
        upper_clip (float): The percentage for upper clipping
        lower_clip (float): The percentage for lower clipping
        flag_value_cols: (List[str]): The names of the columns to flag for outliers

    Returns:
        df (pd.DataFrame): The full dataset with flag columns indicating outliers.
    """
    AutoOutlierLogger.info("Starting outlier detection...")

    # Validate the outlier configuration settings
    validate_config(upper_clip, lower_clip, flag_value_cols)

    # loop through all columns to be flagged for outliers
    for value_col in flag_value_cols:
        # to_numeric is needed to convert strings. However 'coerce' means values that
        # can't be converted are represented by NaNs.
        # TODO data validation and cleaning should replace the need for 'to_numeric'
        # check ticket (RDRP-386)
        df[value_col] = pd.to_numeric(df[value_col], errors="coerce")

        # Call function to add a flag for auto outliers in column value_col
        df = flag_outliers(df, upper_clip, lower_clip, value_col)

        # Log infomation on the number of outliers in column value_col
        log_outlier_info(df, value_col)

    # create 'master' outlier column- which is True if any of the other flags is True
    df = decide_outliers(df, flag_value_cols)

    # log the number of True flags in the master outlier flag column
    num_flagged = df[df["auto_outlier"]]["auto_outlier"].count()

    msg = f"Outlier flags have been created for {num_flagged} records in total."
    AutoOutlierLogger.info(msg)

    AutoOutlierLogger.info("Finishing automatic outlier detection.")

    return df


def apply_short_form_filters(
    df: pd.DataFrame,
    form_type_no: str = "0006",
    sel_type: str = "P",
):

    # Filter to the correct form type
    filtered_df = df[
        (df["formtype"] == form_type_no) & (df["selectiontype"] == sel_type)
    ]

    return filtered_df
