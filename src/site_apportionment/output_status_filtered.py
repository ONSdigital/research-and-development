"""The main file for the Outputs module."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, List, Any


StatusFilteredLogger = logging.getLogger(__name__)


def calc_weighted_intram_tot(
    df: pd.DataFrame, imp_markers_to_keep: List[str], intram_totals: Dict
) -> Dict[str, int]:
    """Calculate the weighted and unweighted intramural totals.

    Args:
        df (pd.DataFrame): The dataframe to calculate the totals for.
        imp_markers_to_keep (List[str]): The list of imp_markers to keep.
        intram_totals (Dict): The dictionary to store the totals in.

    Returns:
        Dict[str, int]: Intramural totals for unweighted and estimated values.
    """
    filtered_df = df.copy().loc[df.imp_marker.isin(imp_markers_to_keep)]

    intram_tot_est = round((filtered_df["211"] * filtered_df["a_weight"]).sum(), 0)

    # check whether the dictionary is empty meaning this is the first time the
    # function is called
    if not intram_totals:
        intram_totals["estimated"] = intram_tot_est

    elif "estimated" in intram_totals:
        intram_totals["apportioned"] = intram_tot_est

    return intram_totals


def save_removed_markers(
    df: pd.DataFrame, imp_markers_to_keep: List[str]
) -> pd.DataFrame:
    """Filter rows neither clear nor imputed for output QA, based on imp_marker."""
    to_remove = ~df["imp_marker"].isin(imp_markers_to_keep)
    return df.copy().loc[to_remove]


def keep_good_markers(
    df: pd.DataFrame,
    imp_markers_to_keep: List[str],
) -> pd.DataFrame:
    """Keep only rows that are clear or imputed, based on the imp_marker column."""
    series_to_keep = df["imp_marker"].isin(imp_markers_to_keep)
    return df.copy().loc[series_to_keep]


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

    output_path = config["outputs_paths"]["outputs_master"]

    tdate = datetime.now().strftime("%y-%m-%d")
    survey_year = config["years"]["survey_year"]
    filename = f"{survey_year}_status_filtered_qa_{tdate}_v{run_id}.csv"
    write_csv(f"{output_path}/output_status_filtered_qa/{filename}", filtered_df)

    StatusFilteredLogger.info("Finished status filtered output.")
