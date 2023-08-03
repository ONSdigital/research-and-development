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
        df (pd.DataFrame): The dataframe used for finding outliers
        upper_clip (float): The percentage for upper clipping as float
        lower_clip (float): The percentage for lower clipping as float
        groupby_cols (list[str]): The columns to be grouped by
        value_col (str): The name of the column outliers are calculated for
    returns:
        df (pd.DataFrame): The dataframe with a outlier flag column.
    """
    lower_band = 0 + lower_clip
    upper_band = 1 - upper_clip

    # Note: selectiontype=='P' doesn't exist in anonymised data!
    filter_cond = (df[value_col] > 0) & (df.selectiontype=='P')
    filtered_df = df[filter_cond]

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

    df = (df.merge(quantiles_up, on=groupby_cols, how='left')
            .merge(quantiles_low, on=groupby_cols, how='left'))

    outlier_cond = (df[value_col] > df.upper_band) | (df[value_col] < df.lower_band)

    # create boolean auto_outlier col based on conditional logic
    df["auto_outlier"] = outlier_cond & filter_cond
    df = df.drop(["upper_band", "lower_band"], axis=1)
    return df


def calc_num_outliers(df: pd.DataFrame, outlier_val_col: str):
    """Calculate and number of outliers flagged.
    
    Also calculate the total number of non-zero entries in the 
    outlier value column.

    args:
        df (pd.DataFrame): The dataframe used for finding outliers
        value_col (str): The name of the column outliers are calculated for

    returns:
        num_flagged (int): The number of flagged outliers
        tot_nonzero (int): The total number of nonzero entries in the 
            outlier value column.
    """

    num_flagged = df[df["auto_outlier"]==True]["auto_outlier"].count()

    tot_nonzero = df[df[outlier_val_col]>0][outlier_val_col].count()
    
    return num_flagged, tot_nonzero


def auto_clipping(df: pd.DataFrame, 
                  upper_clip: float = 0.5, 
                  lower_clip: float = 0.0):
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
    outlier_val_col = "701"

    df[outlier_val_col] = pd.to_numeric(df[outlier_val_col], errors='coerce')
    groupby_cols = ["period", "cellnumber"]

    # Calculate flags for auto outliers
    flagged_df = outlier_flagging(
        df, upper_clip, lower_clip, groupby_cols, outlier_val_col
    )

    num_flagged, tot_nonzero = calc_num_outliers(flagged_df, outlier_val_col)

    msg = (f"{num_flagged} outliers were detected out of a total of "
           f"{tot_nonzero} non-zero entries in column {outlier_val_col}")

    OutlierMainLogger.info("Finishing automatic outlier detection.")  
    OutlierMainLogger.info(msg)

    return flagged_df
