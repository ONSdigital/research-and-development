"""Create the headcount and FTE columns for the NI data."""
import logging
import pandas as pd
import numpy as np

from src.imputation import apportionment as appt

NIheadcountFTELogger = logging.getLogger(__name__)


def create_ni_headcount_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Create and populate the headcount columns for the NI data.

    Example:
        df["headcount_res_m"] = df["501"]

    Args:
        df (pd.DataFrame): The full NI dataframe.

    Returns:
        df (pd.DataFrame): The same data frame with new headcount columns
    """
    # hc_dict is a dictionary from the apportionment module.
    # it is of the form {new_headcount_col : old_5xx_col}
    new_hc_cols = list(appt.hc_dict.keys())

    df = df.copy()

    for hc_col in new_hc_cols:
        # get the name of the old column from the dictionary
        old_col = appt.hc_dict[hc_col]
        # assign the new column to be equal to the old one.
        df[hc_col] = df[old_col]

    df["headcount_total"] = df["headcount_tot_m"] + df["headcount_tot_f"]

    return df


def create_ni_fte_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Create and populate the FTE columns for the NI data.

    Example:
        df["emp_researcher"] = df["405"] if df["200"] == "C"
        df["emp_researcher"] = df["406"] if df["200"] == "D"

    Args:
        df (pd.DataFrame): The full NI dataframe.

    Returns:
        df (pd.DataFrame): The same data frame with new FTE columns
    """
    # fte_dict is a dictionary from the apportionment module.
    # it is of the form {new_fte_col : [old_col_civil, old_col_defence]}
    new_fte_cols = list(appt.fte_dict.keys())

    df = df.copy()
    for col in new_fte_cols:
        df[col] = np.nan

    for fte_col in new_fte_cols:
        # create variable names for the old columns for the civil and defence cases
        civil_col = appt.fte_dict[fte_col][0]
        defence_col = appt.fte_dict[fte_col][1]
        # assign the new column depending on whether the reference is civil or defence
        df.loc[(df["200"] == "C"), fte_col] = df[civil_col]
        df.loc[(df["200"] == "D"), fte_col] = df[defence_col]

    return df


def run_ni_headcount_fte(df: pd.DataFrame) -> pd.DataFrame:
    """Run functions to create headcount and FTE cols for NI data.

    Args:
        df (pd.DataFrame): The full NI dataframe.

    Returns:
        df (pd.DataFrame): The same data frame with new headcount and FTE columns
    """
    NIheadcountFTELogger.info("Creating headcount and FTE columns for NI data.")

    df = create_ni_headcount_cols(df)
    df = create_ni_fte_cols(df)

    return df
