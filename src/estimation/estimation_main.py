"""Main file for the estimation module."""

import logging
from typing import Any
from collections.abc import Callable
import pandas as pd

from src.estimation import apply_weights as appweights
from src.estimation import calculate_weights as weights
from src.utils.helpers import filename_amender

EstMainLogger = logging.getLogger(__name__)


def run_estimation(
    df: pd.DataFrame,
    config: dict[str, Any],
    write_csv: Callable,
) -> pd.DataFrame:
    """
    Run the estimation module.

    Args:
        df (pd.DataFrame): The main dataset were estimation will be applied.
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.

    Returns:
        pd.DataFrame: The main dataset after the application of estimation.
    """
    EstMainLogger.info("Starting estimation weights calculation...")

    # # clean and create a dictionary from the cellno mapper
    # cell_unit_dict = cmap.cellno_unit_dict(cellno_df)

    # calculate the weights
    weighted_df, qa_df = weights.calculate_weighting_factors(df)

    # apply the weights to the dataframe and apply the specified rounding
    for_est = weighted_df.copy()
    estimated_df = appweights.apply_weights(for_est, config, for_qa=True, round_val=4)

    if config["global"]["output_estimation_qa"]:
        EstMainLogger.info("Outputting estimation QA file.")
        est_qa_path = config["estimation_paths"]["qa_path"]
        cell_qa_filename = filename_amender("estimation_weights_qa", config)
        full_qa_filename = filename_amender("full_estimation_qa", config)
        write_csv(f"{est_qa_path}/{cell_qa_filename}", qa_df)
        write_csv(f"{est_qa_path}/{full_qa_filename}", estimated_df)
    EstMainLogger.success("Finished estimation weights calculation.")

    return weighted_df
