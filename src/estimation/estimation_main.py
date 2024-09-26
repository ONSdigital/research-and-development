"""Main file for the estimation module."""
import logging
from datetime import datetime
from typing import Any, Callable, Dict
import pandas as pd

from src.estimation import apply_weights as appweights
from src.estimation import calculate_weights as weights

EstMainLogger = logging.getLogger(__name__)


def run_estimation(
    df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
) -> pd.DataFrame:
    """
    Run the estimation module.

    Args:
        df (pd.DataFrame): The main dataset were estimation will be applied.
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
            This will be the s3, hdfs or network version depending on settings.
        run_id (int): The current run id

    Returns:
        pd.DataFrame: The main dataset after the application of estimation.
    """
    EstMainLogger.info("Starting estimation weights calculation...")

    # # clean and create a dictionary from the cellno mapper
    # cell_unit_dict = cmap.cellno_unit_dict(cellno_df)

    # calculate the weights
    df, qa_df = weights.calculate_weighting_factor(df)

    # calculate the weights for outliers
    weighted_df = weights.outlier_weights(df)

    # apply the weights to the dataframe and apply the specified rounding
    for_est = weighted_df.copy()
    estimated_df = appweights.apply_weights(for_est, config, for_qa=True, round_val=4)

    if config["global"]["output_estimation_qa"]:
        EstMainLogger.info("Outputting estimation QA file.")
        tdate = datetime.now().strftime("%y-%m-%d")
        survey_year = config["years"]["survey_year"]
        est_qa_path = config["estimation_paths"]["qa_path"]
        cell_qa_filename = f"{survey_year}_estimation_weights_qa_{tdate}_v{run_id}.csv"
        full_qa_filename = f"{survey_year}_full_estimation_qa_{tdate}_v{run_id}.csv"
        write_csv(f"{est_qa_path}/{cell_qa_filename}", qa_df)
        write_csv(f"{est_qa_path}/{full_qa_filename}", estimated_df)
    EstMainLogger.info("Finished estimation weights calculation.")

    return weighted_df
