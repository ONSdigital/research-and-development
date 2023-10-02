"""This code could eventually be added to tmi_imputation.py this 
doesn't impact on the readability of the existing code. """

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple

from src.imputation import tmi_imputation as tmi


def create_civdef_dict(
    df: pd.DataFrame, 
    target_variable_list: List[str]
) -> Dict[str, float]:
    """Create dictionary with values to use for civil and defence imputation.

    Args:
        df (pd.DataFrame): The dataframe of 'clear' responses for the given 
            imputation class
        target_variable List(str): List of target variables for which the mean is 
            to be evaluated. 

    Returns:
        Dict[str, float]
    """



def impute_civil_defence(
    df: pd.DataFrame,
    target_variable_list: List[str]
) -> pd.DataFrame:
    """Impute the R&D type for non-responders and 'No R&D'.

    Args:
        df (pd.DataFrame): SPP dataframe afer PG imputation.

    Returns:
        pd.DataFrame: The original dataframe with imputed values for 
            R&D type (civil or defence)
    """
    df = tmi.create_imp_class_col(df, "201", "rusic", "pg_sic_class")
    return df