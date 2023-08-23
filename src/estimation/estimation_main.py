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
    run_id: str,
    ) -> pd.DataFrame:
    """
    Run the estimation module.

    Args:
        df (pd.DataFrame): The main dataset were estimation will be applied.
        cellno_df (pd.DataFrame): The cellno mapper

    Returns:
        pd.DataFrame: The main dataset after the application of estimation.
    """
    EstMainLogger.info("Starting estimation weights calculation...")

    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    est_path = config[f"{NETWORK_OR_HDFS}_paths"]["estimation_path"]
    # clean and create a dictionary from the cellno mapper
    cell_unit_dict = cmap.cellno_unit_dict(cellno_df)

    weighted_df = weights.outlier_weights(df)
    print(weighted_df[weighted_df["outlier"]].head())

    if config["global"]["output_estimation_qa"]:
        tdate = datetime.now().strftime("%Y-%m-%d")
        filename = f"estimation_qa_{tdate}_v{run_id}.csv"
        write_csv(f"{est_path}/estimation_qa/{filename}", weighted_df)
    EstMainLogger.info("Finished estimation weights calculation.")
