"""Create the headcount and FTE columns for the NI data."""
import logging
import pandas as pd
import numpy as np

from typing import Dict, List

from src.imputation import apportionment as appt

NIheadcountFTELogger = logging.getLogger(__name__)


def create_ni_headcount_cols(df:pd.DataFrame) -> pd.DataFrame:
    """Create and populate the headcount columns for the NI data."""
    new_hc_cols = list(appt.hc_dict.keys())

    df = df.copy()
    df[new_hc_cols] = np.nan

    for hc_col in new_hc_cols:
        old_col = appt.hc_dict[hc_col]
        df[hc_col] = df[old_col]

    return df


def create_ni_fte_cols(df:pd.DataFrame) -> pd.DataFrame:
    """Create and populate the headcount columns for the NI data."""
    new_fte_cols = list(appt.fte_dict.keys())

    df = df.copy()
    df[new_fte_cols] = np.nan

    for fte_col in new_fte_cols:
        civil_col = appt.fte_dict[fte_col][0]
        defence_col = appt.fte_dict[fte_col][1]
        df.loc[(df["200"] == "C"), fte_col] = df[civil_col]
        df.loc[(df["200"] == "D"), fte_col] = df[defence_col]

    return df


def run_ni_headcount_fte(df:pd.DataFrame) -> pd.DataFrame:
    """Run functions to create headcount and FTE cols for NI data."""
    NIheadcountFTELogger.info("Creating headcount and FTE columns for NI data.")

    df = create_ni_headcount_cols(df)
    df = create_ni_fte_cols(df)

    return df