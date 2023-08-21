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

    # log the difference in the number of True flags in the final outlier column
    man_true_flagged = df[df["outlier"]]["outlier"].count()
    auto_true_flagged = df[df["auto_outlier"]]["auto_outlier"].count()

    outlier_diff = abs(auto_true_flagged - man_true_flagged)
    msg = f"Outlier flags have been updated for {outlier_diff} records in total."
    ManualOutlierLogger.info(msg)

    ManualOutlierLogger.info("Finalised outlier decisions have been implemented.")

    return df
