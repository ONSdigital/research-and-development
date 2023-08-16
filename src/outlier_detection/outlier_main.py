"""Main file for the Outlier Detection module."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable

from src.outlier_detection import auto_outliers as auto
from src.outlier_detection import manual_outliers as manual

OutlierMainLogger = logging.getLogger(__name__)


def run_outliers(
    df: pd.DataFrame,
    df_manual_supplied: pd.DataFrame,
    config: dict,
    write_csv: Callable,
) -> pd.DataFrame:
    """
    Run the outliering module.

    The auto-outlier procedure is applied first, adding a flag column for
    automatically detected outliers. The data is then output for the user.

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
    flag_cols = config["outliers"]["flag_cols"]
    df_auto_flagged = auto.run_auto_flagging(df, upper_clip, lower_clip, flag_cols)

    OutlierMainLogger.info("Finished Auto Outlier Detection.")
    df_auto_flagged = df_auto_flagged.drop(["manual_outlier"], axis=1)
    outlier_df = df_auto_flagged.merge(
        df_manual_supplied, on=["reference", "instance", "auto_outlier"], how="left"
    )

    # update outlier flag column with manual outliers

    OutlierMainLogger.info("Starting Manual Outlier Detection")
    flagged_outlier_df = manual.apply_manual_outliers(outlier_df)
    OutlierMainLogger.info("Finished Manual Outlier Detection")

    # output the outlier flags for QA
    # TODO when working on DAP need to output QA there also.
    if config["global"]["network_or_hdfs"] == "network":
        OutlierMainLogger.info("Starting output of Outlier QA data...")
        folder = config["network_paths"]["outliers_path"]
        tdate = datetime.now().strftime("%Y-%m-%d")
        filename = f"outliers_qa_{tdate}.csv"
        write_csv(f"{folder}/outliers_qa/{filename}", flagged_outlier_df)
        OutlierMainLogger.info("Finished QA output of outliers data.")

    return flagged_outlier_df
