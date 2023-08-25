""""Apply the esimation weights to short form questions"""
import pandas as pd
import logging


AppWeights_Logger = logging.getLogger(__name__)

def apply_weights(df: pd.DataFrame, round_val: int=4) -> pd.DataFrame:
    """Apply the estimation weights to short from questions.

    Args:
        df (pd.DataFrame): The dataframe weights are calculated for.
        round_val (int): The number of dec places we round to

    Returns:
        pd.DataFrame: The dataframe with the estimated values.
    """
    # list the short form numeric columns the weights are applied to.
    cols_list = ["701", "702", "703", "704", "705", "706", "707", "709", "710", "711"] #noqa

    #TODO remove the forcing to numeric when validation fixed
    for col in cols_list:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[f"{col}_estimated"] = round(df[col]*df["a_weight"], round_val)

    return df

