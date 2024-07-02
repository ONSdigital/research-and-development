"""The main file for the Outputs module."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, List, Any


StatusFilteredLogger = logging.getLogger(__name__)


def save_removed_markers(
    df: pd.DataFrame, imp_markers_to_keep: List[str]
) -> pd.DataFrame:
    """Filter rows neither clear nor imputed for output QA, based on imp_marker."""
    to_remove = ~df["imp_marker"].isin(imp_markers_to_keep)
    return df.copy().loc[to_remove]


def output_status_filtered(
    df: pd.DataFrame,
    imp_markers_to_keep: List[str],
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
):
    """Output rows that are neither clear nor imputed, before these are removed.

    Args:
        df (pd.DataFrame): Dataframe containing records filtered out from outputs.
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id
    """
    StatusFilteredLogger.info("Starting status filtered output...")

    # filter the dataframe
    filtered_df = save_removed_markers(df, imp_markers_to_keep)

    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    paths = config[f"{NETWORK_OR_HDFS}_paths"]
    output_path = paths["output_path"]

    tdate = datetime.now().strftime("%y-%m-%d")
    survey_year = config["years"]["current_year"]
    filename = f"{survey_year}_status_filtered_qa_{tdate}_v{run_id}.csv"
    write_csv(f"{output_path}/output_status_filtered_qa/{filename}", filtered_df)

    StatusFilteredLogger.info("Finished status filtered output.")


def keep_good_markers(
    df: pd.DataFrame,
    imp_markers_to_keep: List[str],
) -> pd.DataFrame:
    """Keep only rows that are clear or imputed, based on the imp_marker column."""
    series_to_keep = df["imp_marker"].isin(imp_markers_to_keep)
    return df.copy().loc[series_to_keep]
