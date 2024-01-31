"""The main file for the Outputs module."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any

OutputMainLogger = logging.getLogger(__name__)


def remove_unwanted_records(
    df: pd.DataFrame,
    config: Dict,
    write_csv: Callable,
    run_id: int,
) -> pd.DataFrame:
    """Remove records with 'no imputation' or 'no mean found'.

      Args:
        df (pd.DataFrame): Dataframe after site apportionment.
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id

    Returns:
        (pd.DataFrame): Dataframe with the unwanted records removed
    """
    # Keep only imputed, constructed and clear records ("R")
    imp_markers_to_keep = ["R", "TMI", "CF", "MoR", "constructed"]
    to_keep = df["imp_marker"].isin(imp_markers_to_keep)

    to_keep_df = df.copy().loc[to_keep]
    filtered_output_df = df.copy().loc[~to_keep]

    # change the value of the status column to 'imputed' for imputed statuses
    condition = to_keep_df["imp_marker"].isin(imp_markers_to_keep)
    to_keep_df.loc[condition, "status"] = "imputed"

    # Running status filtered full dataframe output for QA
    if config["global"]["output_status_filtered"]:
        OutputMainLogger.info("Starting status filtered output...")
        output_status_filtered(
            filtered_output_df,
            config,
            write_csv,
            run_id,
        )
        OutputMainLogger.info("Finished status filtered output.")

    return to_keep_df

def output_status_filtered(
    df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
):
    """Output the removed records with 'no imputation' or 'no mean found'.

    Args:
        df (pd.DataFrame): Dataframe containing records filtered out from outputs.
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id

    Returns:
        (pd.DataFrame): Dataframe with the unwanted records removed
    """

    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    paths = config[f"{NETWORK_OR_HDFS}_paths"]
    output_path = paths["output_path"]

    tdate = datetime.now().strftime("%Y-%m-%d")
    filename = f"status_filtered_qa_{tdate}_v{run_id}.csv"
    write_csv(f"{output_path}/output_status_filtered_qa/{filename}", df)
