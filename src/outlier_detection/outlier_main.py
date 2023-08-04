"""Main file for the Outlier Detection module."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable

from src.outlier_detection import auto_outliers as auto

OutlierMainLogger = logging.getLogger(__name__)


def run_outliers(df: pd.DataFrame, config: dict, write_csv: Callable):
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
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
            This will be the hdfs or network version depending on settings.

    Returns:
        df_outliers_applied (pd.DataFrame): The main dataset with a flag column
            indicating outliers for use the estimation module.
    """
    OutlierMainLogger.info("Starting Auto Outlier Detection...")

    upper_clip = config["outliers"]["upper_clip"]
    lower_clip = config["outliers"]["lower_clip"]
    df_auto_flagged = auto.auto_clipping(df, upper_clip, lower_clip)

    OutlierMainLogger.info("Finished Auto Outlier Detection.")

    # output the outlier flags for QA
    if config["global"]["network_or_hdfs"] == "network":
        OutlierMainLogger.info("Starting output of Outlier QA data...")
        folder = config["network_paths"]["outliers_path"]
        tdate = datetime.now().strftime("%Y-%m-%d")
        filename = f"outliers_qa_{tdate}.csv"
        write_csv(f"{folder}/outliers_qa/{filename}", df_auto_flagged)
        OutlierMainLogger.info("Finished QA output of outliers data.")

     # read in file for manual outliers

     # update outlier flag column with manual outliers   

    return df_auto_flagged