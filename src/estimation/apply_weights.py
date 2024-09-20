""""Apply the esimation weights to short form questions"""
import logging
from typing import Dict, Any
from itertools import chain

import pandas as pd

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
    # generate a list of all the columns the weights should be applied to.
    num_cols = config["estimation"]["numeric_cols"]  # the 7xx cols
    master_cols = ["211", "305", "emp_total", "headcount_total"]
    hc_tot_cols = ["headcount_tot_m", "headcount_tot_f"]
    bd_qs_lists = list(config["breakdowns"].values())
    bd_cols = list(chain(*bd_qs_lists))  # breakdown cols 2xx, 3xx, emp_xx hc_xx
    cols_list = num_cols + master_cols + hc_tot_cols + bd_cols

    # TODO remove the forcing to numeric when validation fixed
    # if the dataframe is for QA output, create new columns with the weights applied.
    if for_qa:
        for col in cols_list:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[f"{col}_estimated"] = round(df[col] * df["a_weight"], round_val)
    # if the dataframe is for the final output, apply the weights to the original cols.
    else:
        for col in cols_list:
            df[col] = round(df[col] * df["a_weight"], round_val)

    return df
