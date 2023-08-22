"""Main file for the estimation module."""
import logging
import pandas as pd

from src.estimation import calculate_weights as weights

EstMainLogger = logging.getLogger(__name__)


def run_estimation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run the estimation module.

    Args:
        df (pd.DataFrame): The main dataset were estimation will be applied.

    Returns:
        pd.DataFrame: The main dataset after the application of estimation.
    """
    EstMainLogger.info("Starting estimation weights calculation...")
    df = weights.outlier_weights(df)
    print(df.head())
    EstMainLogger.info("Finished estimation weights calculation.")

    
