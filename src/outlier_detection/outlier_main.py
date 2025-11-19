"""Main file for the Outlier Detection module."""

import logging
import pandas as pd
from typing import Any
from collections.abc import Callable

from src.outlier_detection import auto_outliers as auto
from src.outlier_detection import manual_outliers as manual
from src.utils.helpers import filename_amender

OutlierMainLogger = logging.getLogger(__name__)


def run_outliers(
    df: pd.DataFrame,
    df_manual_supplied: pd.DataFrame,
    config: dict[str, Any],
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
        df_manual_supplied (pd.DataFrame): Dataframe with manual outlier flags
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
            This will be the s3, hdfs or network version depending on settings.

    Returns:
        df_outliers_applied (pd.DataFrame): The main dataset with a flag column
            indicating outliers for use the estimation module.
    """
    OutlierMainLogger.info("Starting Auto Outlier Detection...")

    upper_clip = config["outliers"]["upper_clip"]
    lower_clip = config["outliers"]["lower_clip"]
    flag_cols = config["outliers"]["flag_cols"]

    outlier_qa_path = config["outliers_paths"]["qa_path"]
    auto_outlier_path = config["outliers_paths"]["auto_outliers_path"]

    # Calculate automatic outliers
    df_auto_flagged = auto.run_auto_flagging(df, upper_clip, lower_clip, flag_cols)

    # Apply short form filters before output
    filtered_df = auto.apply_short_form_filters(df_auto_flagged)

    # Output the file with auto outliers for manual checking
    if config["global"]["output_auto_outliers"]:
        OutlierMainLogger.info("Starting the output of the automatic outliers file")
        filename = filename_amender("auto_outlier", config)
        file_path = auto_outlier_path + filename
        write_csv(file_path, filtered_df)
        OutlierMainLogger.success("Finished writing CSV to %s", auto_outlier_path)
    else:
        OutlierMainLogger.info("Skipping the output of the automatic outliers file")

    # If we don't load manual outliers then `df_manual_supplied = None`. For the code to
    # continue to run, we set the manual file to be equal to the auto output and filter
    # the relevant columns. This way we don't filter out any manual outliers.
    if not config["global"]["load_manual_outliers"]:
        df_manual_supplied = pd.DataFrame(
            columns=["reference", "manual_outlier", "auto_override_outlier_status"]
        )
        OutlierMainLogger.info(
            "Skipping loading of manual outliers. manual_outlier column treated as NaN"
        )

    # update outlier flag column with manual outliers
    OutlierMainLogger.info("Starting Manual Outlier Application")
    outlier_df = df_auto_flagged.merge(df_manual_supplied, on=["reference"], how="left")
    flagged_outlier_df = manual.apply_manual_outliers(outlier_df)
    OutlierMainLogger.success("Finished Manual Outlier Application")

    # Output the outlier flags for QA
    if config["global"]["output_outlier_qa"]:
        OutlierMainLogger.info("Starting output of Outlier QA data...")
        filename = filename_amender("outliers_qa", config)
        write_csv(f"{outlier_qa_path}/{filename}", flagged_outlier_df)
        OutlierMainLogger.success("Finished QA output of outliers data.")
    else:
        OutlierMainLogger.info("Skipping output of Outlier QA data...")

    # Return clean dataframe to pipline
    drop_cols = [f"{col}_outlier_flag" for col in flag_cols]

    flagged_outlier_df = flagged_outlier_df.drop(drop_cols, axis=1)

    return flagged_outlier_df
