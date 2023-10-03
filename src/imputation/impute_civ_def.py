"""This code could eventually be added to tmi_imputation.py this 
doesn't impact on the readability of the existing code. """

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple

from src.imputation import tmi_imputation as tmi


def calc_cd_proportions(df: pd.DataFrame): # -> Tuple[float, float]:
    """Calc the proportion of civil and defence entries in a df"""
    num_civ = len(df.loc[df["200"] == 'C', "200"])
    num_def = len(df.loc[df["200"] == 'D', "200"])

    proportion_civ = num_civ/(num_civ + num_def)
    proportion_def = num_def/(num_civ + num_def)

    print(proportion_civ)

    return proportion_civ, proportion_def


def create_civdef_dict(
    df: pd.DataFrame
) -> Tuple[Dict[str, float], pd.DataFrame]:
    """Create dictionary with values to use for civil and defence imputation.

    Args:
        df (pd.DataFrame): The dataframe of 'clear' responses for the given 
            imputation class

    Returns:
        Dict[str, Tuple(float, float)]
    """
    # create dictionary to hold civil or defence ratios for each class
    civdef_dict = {}

    # create imputation classes based on product group and rusic
    df = tmi.create_imp_class_col(df, "201", "rusic", "pg_sic_class")

    # create column to record whether a group is empty
    df["empty_group"] = False

    # Filter out imputation classes that are missing either "201" or "rusic"
    filtered_df = df[~(df["pg_sic_class"].str.contains("nan"))]

    # Group by the imputation class the loop through the groups
    pg_sic_grp = filtered_df.loc[filtered_df["instance"] != 0].groupby("pg_sic_class")

    for pg_sic_class, class_group in pg_sic_grp:

        cond = class_group["200"].isin(["C", "D"])
        if class_group.loc[cond].empty:
            class_group["empty_group"] = True
        else:
            # for each imp class, genearte civil and defence proportions
            civdef_dict[pg_sic_class] = calc_cd_proportions(class_group)
    
    return civdef_dict, df


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
    # Filter for clear statuses
    clear_statuses = ["210", "211"]
    clear_df = tmi.filter_by_column_content(df, "statusencoded", clear_statuses)

    # # Filter out imputation classes that are missing either "200" or "201"
    # clear_df = clear_df[~(clear_df["imp_class"].str.contains("nan"))]

    # create a dict mapping each class to either 'C' (civil) or 'D'(defence)
    civil_def_dict = create_civdef_dict(clear_df)

    return df