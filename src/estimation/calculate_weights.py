import pandas as pd
import numpy as np
import logging


CalcWeights_Logger = logging.getLogger(__name__)

def outlier_weights(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate weights for outliers.

    If a reference has been flagged as an outlier,
    the a weight value is set to 1.and

    Args:
        df (pd.DataFrame): The dataframe weights are calculated for.

    Returns:
        pd.DataFrame: The dataframe with the a_weights set to 1 for outliers.
    """
    df["a_weight"] = np.where(df["auto_outlier"], 1.0, df["a_weight"])

    return df

