"""Main file for the estimation module."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any

from src.estimation import calculate_weights as weights
from src.estimation import cellno_mapper as cmap

EstMainLogger = logging.getLogger(__name__)


def run_estimation(
    df: pd.DataFrame,
    cellno_df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
) -> pd.DataFrame:
    """
    Run the estimation module.

    Args:
        df (pd.DataFrame): The main dataset were estimation will be applied.
        cellno_df (pd.DataFrame): The cellno mapper
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
            This will be the hdfs or network version depending on settings.
        run_id (int): The current run id

    Returns:
        pd.DataFrame: The main dataset after the application of estimation.
    """
    EstMainLogger.info("Starting estimation weights calculation...")

    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    est_path = config[f"{NETWORK_OR_HDFS}_paths"]["estimation_path"]

    # clean and create a dictionary from the cellno mapper
    cell_unit_dict = cmap.cellno_unit_dict(cellno_df)

    # calculate the weights
    df, qa_df = weights.calculate_weighting_factor(df, cell_unit_dict)

    # calculate the weights for outliers
    weights.outlier_weights(df)

    if config["global"]["output_estimation_qa"]:
        EstMainLogger.info("Outputting estimation QA file.")
        tdate = datetime.now().strftime("%Y-%m-%d")
        cell_qa_filename = f"estimation_weights_qa_{tdate}_v{run_id}.csv"
        full_qa_filename = f"full_estimation_qa_{tdate}_v{run_id}.csv"
        write_csv(f"{est_path}/estimation_qa/{cell_qa_filename}", qa_df)
        write_csv(f"{est_path}/estimation_qa/{full_qa_filename}", df)
    EstMainLogger.info("Finished estimation weights calculation.")

    return df
