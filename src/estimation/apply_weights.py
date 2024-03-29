""""Apply the esimation weights to short form questions"""
import logging
from typing import Dict, Any, Tuple, List
from itertools import chain

import pandas as pd

AppWeights_Logger = logging.getLogger(__name__)


def apply_weights(
    df: pd.DataFrame, config: Dict[str, Any], round_val: int = 4
) -> Tuple[pd.DataFrame, List]:
    """Apply the estimation weights to short form questions.

    Args:
        df (pd.DataFrame): The dataframe weights are calculated for.
        config (dict): The configuration settings.
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
    for col in cols_list:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[f"{col}_estimated"] = round(df[col] * df["a_weight"], round_val)

    return df, cols_list
