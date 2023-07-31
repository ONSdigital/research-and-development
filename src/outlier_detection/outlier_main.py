"""Main file for the Outlier Detection module."""
import logging
import pandas as pd

from src.outlier_detection import auto_outliers as auto

OutlierMainLogger = logging.getLogger(__name__)


def run_outliers(df: pd.DataFrame, outlier_config: dict):
    """
    Run the outliering module.

    The auto-outlier procedure is applied first, adding a flag column for
    automatically detected outliers. This is output for the user.

    If a manual outlier file has been supplied by the user, this is read in,
    and the manually specified outlier flags supercede auto ones.

    The dataset is returned with a final outlier flag column to be used in
    the estimation module.

    Args:
        df (pd.DataFrame): The main dataset where outliers are to be calculated.
        outlier_config (dict): The global configuration settings.

    Returns:
        df_outliers_applied (pd.DataFrame): The main dataset with a flag column
            indicating outliers for use the estimation module.
    """
    OutlierMainLogger.info("Starting Auto Outlier Detection...")

    upper_clip = outlier_config["upper_clip"]
    lower_clip = outlier_config["lower_clip"]
    df_auto_flagged = auto.auto_clipping(df, upper_clip, lower_clip)
    print(df_auto_flagged.head())
    OutlierMainLogger.info("Finished Auto Outlier Detection.")
