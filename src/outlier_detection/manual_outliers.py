import logging
import pandas as pd

ManualOutlierLogger = logging.getLogger(__name__)


def apply_manual_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Applies the manual outliers by creating a new column that
    overwrites the automatic flags with a manual outlier flag.

    Args:
        df (pd.DataFrame): The dataframe containing the auto-outlier flags

    Returns:
        pd.DataFrame: Dataframe with a new column
        containing the final decision on outliering
    """

    # Count number of manually flagged outliers
    num_manual_flagged = len(df["manual_outlier"]) - df["manual_outlier"].isnull().sum()
    msg = f"Manual outlier flags have been identified for {num_manual_flagged} records."  # noqa

    # Overwriting auto-outliers with manual outliers
    df["outlier"] = df["manual_outlier"].combine_first(df["auto_outlier"])

    # Log the number of manually applied True flags (only count for instance 0)

    filtered_df = df[df["instance"] == 0]
    
    t_outlier_diff = len(
        filtered_df.loc[(filtered_df["auto_outlier"].isin([False])) & (filtered_df["outlier"].isin([True]))]
    )
    msg = f"{t_outlier_diff} record(s) have been manually updated as True."
    ManualOutlierLogger.info(msg)

    # Log the number of manually applied False flags
    f_outlier_diff = len(
        filtered_df.loc[(filtered_df["auto_outlier"].isin([True])) & (filtered_df["outlier"].isin([False]))]
    )
    msg = f"{f_outlier_diff} record(s) have been manually updated as False."
    ManualOutlierLogger.info(msg)

    ManualOutlierLogger.info("Finalised outlier decisions have been implemented.")

    return df
