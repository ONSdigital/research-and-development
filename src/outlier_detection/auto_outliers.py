"""Apply outlier detection to the dataset."""
import logging
import pandas as pd

OutlierMainLogger = logging.getLogger(__name__)


def validate_config(upper_clip: float, lower_clip: float):
    """Validate the outlier config settings.

    args:
        upper_clip (float): The percentage for upper clipping as float
        lower_clip (float): The percentage for lower clipping as float

    raises:
        ImportError if upper_clip or lower_clip have invalid values
    """
    OutlierMainLogger.info(f"Upper clip: {upper_clip}, Lower clip: {lower_clip}")

    if (upper_clip == 0) and (lower_clip == 0):
        OutlierMainLogger.warning(
            "upper_clip and lower_clip both zero: "
            "All auto_outlier flags will be False"
        )
    elif (upper_clip < 0) | (lower_clip < 0):
        OutlierMainLogger.error("upper_clip and lower_clip cannot be negative")
        raise ImportError
    elif upper_clip + lower_clip > 1.0:
        OutlierMainLogger.error("upper_clip and lower_clip must sum to < 1")
        raise ImportError


def outlier_flagging(
    df: pd.DataFrame,
    upper_clip: float,
    lower_clip: float,
    groupby_cols: list = ["period, cellnumber"],
    value_col: str = "211",
):
    """Create Boolean column to flag outliers.

    args:
        df (pd.DataFrame): filtered dataframe containing only essential cols
            and rows used for auto outliers
        upper_clip (float): The percentage for upper clipping as float
        lower_clip (float): The percentage for lower clipping as float

    returns:
        df (pd.DataFrame): The dataframe with a outlier flag column.
    """
    lower_band = 0 + lower_clip
    upper_band = 1 - upper_clip

    quantiles_up = (
        df.groupby(groupby_cols)
        .quantile([upper_band], interpolation="nearest")
        .reset_index()
    )
    quantiles_up = quantiles_up.rename(columns={value_col: "upper_band"})[
        groupby_cols + ["upper_band"]
    ]

    quantiles_low = (
        df.groupby(groupby_cols)
        .quantile([lower_band], interpolation="nearest")
        .reset_index()
    )
    quantiles_low = quantiles_low.rename(columns={value_col: "lower_band"})[
        groupby_cols + ["lower_band"]
    ]
    # quantiles_low.round({'value': 0})
    df = df.merge(quantiles_up, on=groupby_cols).merge(quantiles_low, on=groupby_cols)

    df["auto_outlier"] = (df[value_col] > df.upper_band) | (
        df[value_col] < df.lower_band
    )
    return df


def auto_clipping(df: pd.DataFrame, upper_clip: float = 0.1, lower_clip: float = 0.0):
    """Flag outliers for quantiles specified in the config.


    Calculate the automatic outlier flag (Boolean) in a column
    called auto_outlier.

    Use values specified in the config file for the upper and lower
    clipping values.

    Notes:
        - The value column for the detection of outliers is assumed to be '211'.
        - Outliering is not applied to 'census' data, so flags are only created
            where 'seclectiontype' = 'P'.

    Args:
        df (pd.DataFrame): The main dataset where outliers are to be calculated
        upper_clip (float): The percentage for upper clipping as float
        lower_clip (float): The percentage for lower clipping as float

    Returns:
        df (pd.DataFrame): The full dataset with flag column for outliers.
    """
    OutlierMainLogger.info("Starting outlier detection...")

    # Validate the outlier configuration settings
    validate_config(upper_clip, lower_clip)

    # Specify the value column and the groupby columns for outliers
    outlier_val_col = "211"
    df = df.astype({outlier_val_col: float})
    groupby_cols = ["period", "cellnumber"]

    # Note: selectiontype=='P' doesn't exist in anonymised data!

    # Filter dataframe and select cols needed for outlier flag
    clipping_df = df[df.selectiontype == "P"][groupby_cols + [outlier_val_col]].copy()

    flagged_df = outlier_flagging(
        clipping_df, upper_clip, lower_clip, groupby_cols, outlier_val_col
    )

    return flagged_df
