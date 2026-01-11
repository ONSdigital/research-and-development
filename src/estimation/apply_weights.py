""" "Apply the esimation weights to short form questions"""

import logging
from typing import Dict, Any

# from itertools import chain

import pandas as pd

from src.utils.breakdown_validation import get_all_wanted_columns, calc_totals

AppWeights_Logger = logging.getLogger(__name__)


def apply_weights(
    df: pd.DataFrame, config: Dict[str, Any], for_qa: bool = False, round_val: int = 4
) -> pd.DataFrame:
    """Apply the estimation weights to short form questions.

    Args:
        df (pd.DataFrame): The dataframe weights are calculated for.
        config (dict): The configuration settings.
        for_qa (bool): If True, keep the values before and after weights are applied.
        round_val (int): The number of dec places we round to

    Returns:
        pd.DataFrame: The dataframe with the estimated values.
    """
    # generate lists of the columns the weights should be applied to.
    employment_cols = get_all_wanted_columns(config, "employment")
    all_numeric_cols = get_all_wanted_columns(config, "estimation")

    # if the dataframe is for QA output, create new columns with the weights applied.
    if for_qa:
        for col in all_numeric_cols:
            df[f"{col}_estimated"] = (df[col] * df["a_weight"]).round(round_val)
        for col in employment_cols:
            df[f"{col}_estimated"] = (df[f"{col}_estimated"] * df["g_weight"]).round(0)

    # otherwise, apply the weights directly to the existing columns
    else:
        for col in all_numeric_cols:
            df[col] = (df[col] * df["a_weight"]).round(round_val)
        for col in employment_cols:
            df[col] = (df[col] * df["g_weight"]).round(0)

        df = calc_totals(df, config, "employment", 0)

    return df
