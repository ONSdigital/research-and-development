"""Main file for the Outlier Detection module."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any

from src.outlier_detection import auto_outliers as auto
from src.outlier_detection import manual_outliers as manual

OutlierMainLogger = logging.getLogger(__name__)


def run_outliers(
    df: pd.DataFrame,
    df_manual_supplied: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
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
            This will be the hdfs or network version depending on settings.
        run_id (int): The current run id

    Returns:
        df_outliers_applied (pd.DataFrame): The main dataset with a flag column
            indicating outliers for use the estimation module.
    """
    OutlierMainLogger.info("Starting Auto Outlier Detection...")

    network_or_hdfs = config["global"]["network_or_hdfs"]
    upper_clip = config["outliers"]["upper_clip"]
    lower_clip = config["outliers"]["lower_clip"]
    flag_cols = config["outliers"]["flag_cols"]
    outlier_path = config[f"{network_or_hdfs}_paths"]["outliers_path"]
    auto_outlier_path = outlier_path + "/auto_outliers"

    # Calculate automatic outliers
    df_auto_flagged = auto.run_auto_flagging(df, upper_clip, lower_clip, flag_cols)

    # Apply short form filters before output
    filtered_df = auto.apply_short_form_filters(df_auto_flagged)

    # Output the file with auto outliers for manual checking
    tdate = datetime.now().strftime("%y-%m-%d")
    survey_year = config["years"]["survey_year"]
    if config["global"]["output_auto_outliers"]:
        OutlierMainLogger.info("Starting the output of the automatic outliers file")
        file_path = (
            auto_outlier_path + f"/{survey_year}_manual_outlier_{tdate}_v{run_id}.csv"
        )
        write_csv(file_path, filtered_df)
        OutlierMainLogger.info("Finished writing CSV to %s", auto_outlier_path)
    else:
        OutlierMainLogger.info("Skipping the output of the automatic outliers file")

    # If we don't load manual outliers then `df_manual_supplied = None`. For the code to
    # continue to run, we set the manual file to be equal to the auto output and filter
    # the relevant columns. This way we don't filter out any manual outliers.
    if not config["global"]["load_manual_outliers"]:
        df_manual_supplied = filtered_df[["reference", "manual_outlier"]]
        OutlierMainLogger.info(
            "Skipping loading of manual outliers. manual_outlier column treated as NaN"
        )

    # update outlier flag column with manual outliers
    OutlierMainLogger.info("Starting Manual Outlier Application")
    df_auto_flagged = df_auto_flagged.drop(["manual_outlier"], axis=1)
    outlier_df = df_auto_flagged.merge(df_manual_supplied, on=["reference"], how="left")
    flagged_outlier_df = manual.apply_manual_outliers(outlier_df)
    OutlierMainLogger.info("Finished Manual Outlier Application")

    # Output the outlier flags for QA
    if config["global"]["output_outlier_qa"]:
        OutlierMainLogger.info("Starting output of Outlier QA data...")
        filename = f"{survey_year}_outliers_qa_{tdate}_v{run_id}.csv"
        write_csv(f"{outlier_path}/outliers_qa/{filename}", flagged_outlier_df)
        OutlierMainLogger.info("Finished QA output of outliers data.")
    else:
        OutlierMainLogger.info("Skipping output of Outlier QA data...")

    # Return clean dataframe to pipline
    drop_cols = [f"{col}_outlier_flag" for col in flag_cols]

    flagged_outlier_df = flagged_outlier_df.drop(drop_cols, axis=1)

    return flagged_outlier_df
