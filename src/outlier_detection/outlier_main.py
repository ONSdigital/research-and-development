"""Main file for the Outlier Detection module."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any

from src.outlier_detection import auto_outliers as auto

OutlierMainLogger = logging.getLogger(__name__)


def run_outliers(
    df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_log_num: str,
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
    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    upper_clip = config["outliers"]["upper_clip"]
    lower_clip = config["outliers"]["lower_clip"]
    flag_cols = config["outliers"]["flag_cols"]
    outlier_path = config[f"{NETWORK_OR_HDFS}_paths"]["outliers_path"]
    auto_outlier_path = outlier_path + "/auto_outliers"
    df_auto_flagged = auto.run_auto_flagging(df, upper_clip, lower_clip, flag_cols)

    # Apply short form filters before output
    filtered_df = auto.apply_short_form_filters(df_auto_flagged)

    # Output the auto outlier file
    OutlierMainLogger.info(
        f"Appending runlog number {run_log_num} to the manual outlier file"
    )
    file_path = auto_outlier_path + f"/manual_outlier_{run_log_num}.csv"
    write_csv(file_path, filtered_df)
    OutlierMainLogger.info("Finished writing CSV to %s", auto_outlier_path)

    # Output the outlier flags for QA
    OutlierMainLogger.info("Starting output of Outlier QA data...")
    tdate = datetime.now().strftime("%Y-%m-%d")
    filename = f"outliers_qa_{tdate}.csv"
    write_csv(f"{outlier_path}/outliers_qa/{filename}", df_auto_flagged)
    OutlierMainLogger.info("Finished QA output of outliers data.")

    # read in file for manual outliers

    # update outlier flag column with manual outliers

    # Write out the CSV

    return df_auto_flagged
