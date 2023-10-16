"""This code could eventually be added to tmi_imputation.py this
doesn't impact on the readability of the existing code. """

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple

from src.imputation import tmi_imputation as tmi

clear_statuses = ["Clear", "Clear - overridden"]


def calc_cd_proportions(df: pd.DataFrame):  # -> Tuple[float, float]:
    """Calc the proportion of civil and defence entries

    The proportions are calculated for a dataframe representing a
    single imputation class, which contains at least one non-null
    entry for column 200, so division by zero will be avoided.
    """
    num_civ = len(df.loc[df["200"] == "C", "200"])
    num_def = len(df.loc[df["200"] == "D", "200"])

    proportion_civ = num_civ / (num_civ + num_def)
    proportion_def = num_def / (num_civ + num_def)

    return proportion_civ, proportion_def


def create_civdef_dict(df: pd.DataFrame) -> Tuple[Dict[str, float], pd.DataFrame]:
    """Create dictionaries with values to use for civil and defence imputation.

    Two dictionaries are created, one for imputation classes based on both
    product group and SIC, and the second for imputation classes based
    on product group only.

    Args:
        df (pd.DataFrame): The dataframe of 'clear' responses for the given
            imputation class

    Returns:
        Dict[str, Tuple(float, float)]
    """
    # create dictionary to hold civil or defence ratios for each class
    pgsic_dict = {}

    # Filter out imputation classes that are missing either "201" or "rusic"
    # and exclude empty pg_sic classes
    cond1 = ~(df["pg_sic_class"].str.contains("nan")) & (
        df["empty_pgsic_group"] == False
    )
    filtered_df = df[cond1]

    # Group by the pg_sic imputation class
    pg_sic_grp = filtered_df.groupby("pg_sic_class")

    # loop through the pg_sic imputation class groups
    for pg_sic_class, class_group_df in pg_sic_grp:
        pgsic_dict[pg_sic_class] = calc_cd_proportions(class_group_df)

    # create a second dictionary to hold civil or defence ratios
    # for the "empty_pgsic_group" cases
    pg_dict = {}

    # filter out invalid pg classes and empty pg groups from the original dataframe
    cond2 = ~(df["pg_class"].str.contains("nan")) & (df["empty_pg_group"] == False)
    filtered_df2 = df[cond2]

    # evaluate which pg classes are needed for empty pg_sic groups
    num_empty = filtered_df2.groupby("pg_class")["empty_pgsic_group"].transform(sum)
    filtered_df2 = filtered_df2.loc[num_empty > 0]

    # Group by the pg-only imputation class the loop through the groups
    pg_grp = filtered_df2.groupby("pg_class")

    for pg_class, class_group_df in pg_grp:
        pg_dict[pg_class] = calc_cd_proportions(class_group_df)

    return pgsic_dict, pg_dict


def calc_empty_group(
    df: pd.DataFrame, class_name: str, new_col_name: str
) -> pd.DataFrame:
    """Add a new bool column flagging empty groups."""

    clear_mask = df["status"].isin(clear_statuses)
    df["valid_civdef_val"] = ~df["200"].isnull() & clear_mask

    # calculate the number of valid entries in the class for column 200
    num_valid = df.groupby(class_name)["valid_civdef_val"].transform(sum)

    # exclude classes that are not valid
    valid_class_mask = ~(df[class_name].str.contains("nan"))

    # create new bool column to flag empty classes
    df[new_col_name] = valid_class_mask & (num_valid == 0)

    df = df.drop("valid_civdef_val", axis=1)

    return df


def prep_cd_imp_classes(df: pd.DataFrame) -> pd.DataFrame:
    """Create new columns to use for imputation classes,

    Also create boolean columns to flag for when these contain no
    valid entries that can be used for imputation.
    """
    # create imputation classes based on product group and rusic
    df = tmi.create_imp_class_col(df, "201", "rusic", "pg_sic_class")

    # flag empty pg_sic_classes in new bool col "empty_pgsic_class"
    df = calc_empty_group(df, "pg_sic_class", "empty_pgsic_group")

    # create a new imputation class called "pg_class" based on product group
    # (col 201) only, to be used when imputation cannot be performed using
    # both prodcut group and SIC.
    df = tmi.create_imp_class_col(df, "201", None, "pg_class")

    # flag empty pg_classes in new bool col "empty_pg_class"
    df = calc_empty_group(df, "pg_class", "empty_pg_group")

    return df


def random_assign_civdef(
    df: pd.DataFrame, proportions: Tuple[float, float]
) -> pd.DataFrame:
    """Assign "C" for civil or "D" for defence randomly based on
    the proportions supplied.
    """
    np.random.seed(seed=42)
    df["200_imputed"] = np.random.choice(["C", "D"], size=len(df), p=proportions)
    return df


def apply_civdev_imputation(
    df: pd.DataFrame,
    pgsic_dict: Dict[str, Tuple[float, float]],
    pg_dict: Dict[str, Tuple[float, float]],
) -> pd.DataFrame:
    """Apply imputation for R&D type for non-responders and 'No R&D'.

    Values in column 200 (R&D type) are imputed with either "C" for civl or
    "D" for defence, based on ratios in the same imputation class in
    clear responders.

    This is done in three passes for successively smaller imputation classes:
    - The first class consists of all clear responders
    - The second set of imputation classes are based on product group only
    - The final set of imputation classes are bassed on product group and SIC

    Args:
        df (pd.DataFrame): The dataframe of all responses
        pgsic_dict (Dict[str, Tuple(float, float)]): Dictionary with
            proportions to use in imputation based on product group and SIC.
        pg_dict (Dict[str, Tuple(float, float)]): Dictionary with proportions
            to use in imputation based on product group only.

    Returns:
        pd.DataFrame: The same dataframe with R&D type imputed.
    '"""
    # create temporary column for the imputed value
    df["200_imputed"] = df["200"]
    # Create logic conditions for filtering
    clear_mask = df["status"].isin(clear_statuses)
    to_impute_mask = (df["status"] == "Form sent out") | (df["604"] == "No")

    # PASS 1: find civil and defence proportions for the whole clear dataframe
    clear_df = df.loc[clear_mask].copy()
    proportions = calc_cd_proportions(clear_df)

    # randomly assign civil or defence based on proportions in whole clear df
    to_impute_df = df.loc[to_impute_mask].copy()
    to_impute_df = random_assign_civdef(to_impute_df, proportions)
    to_impute_df["200_imp_marker"] = "fall_back_imputed"

    # PASS 2: refine based on product group imputation class

    # filter out empty and invalid imputation classes
    cond1 = to_impute_df["empty_pg_group"] == False
    cond2 = ~to_impute_df["pg_class"].str.contains("nan")

    filtered_df = to_impute_df.loc[cond1 & cond2].copy()

    # loop through the pg imputation classes to apply imputation
    pg_grp = filtered_df.groupby("pg_class")

    for pg_class, class_group_df in pg_grp:
        if pg_class in pg_dict.keys():
            class_group_df = random_assign_civdef(class_group_df, pg_dict[pg_class])
            class_group_df["200_imp_marker"] = "pg_group_imputed"

        tmi.apply_to_original(class_group_df, filtered_df)

    tmi.apply_to_original(filtered_df, to_impute_df)

    # PASS 3: refine again based on product group and SIC imputation class

    # filter out empty and invalid imputation classes
    cond3 = to_impute_df["empty_pgsic_group"] == False
    cond4 = ~to_impute_df["pg_sic_class"].str.contains("nan")

    filtered_df2 = to_impute_df.loc[cond3 & cond4].copy()

    # loop through the pg_sic imputation classes to apply imputation
    pg_sic_grp = filtered_df2.groupby("pg_sic_class")

    for pg_sic_class, class_group_df in pg_sic_grp:
        if pg_sic_class in pgsic_dict.keys():
            class_group_df = random_assign_civdef(
                class_group_df, pgsic_dict[pg_sic_class]
            )
            class_group_df["200_imp_marker"] = "pg_sic_group_imputed"

        tmi.apply_to_original(class_group_df, filtered_df2)
    tmi.apply_to_original(filtered_df2, to_impute_df)

    updated_df = tmi.apply_to_original(to_impute_df, df)
    updated_df["200"] = updated_df["200_imputed"]

    updated_df = updated_df.drop(["200_imputed", "pg_class"], axis=1)
    return updated_df


def impute_civil_defence(df: pd.DataFrame) -> pd.DataFrame:
    """Impute the R&D type for non-responders and 'No R&D'.

    Args:
        df (pd.DataFrame): SPP dataframe afer product group imputation.

    Returns:
        pd.DataFrame: The original dataframe with imputed values for
            R&D type (civil or defence)
    """
    # create temp QA columns
    df["200_original"] = df["200"]
    df["pg_sic_class"] = "nan_nan"
    df["empty_pgsic_group"] = False
    df["empty_pg_group"] = False
    df["200_imp_marker"] = "no_imputation"

    filtered_df = df.loc[df["instance"] != 0].copy()
    filtered_df = prep_cd_imp_classes(filtered_df)

    # create a dict mapping each class to either 'C' (civil) or 'D'(defence)
    clear_df = filtered_df[filtered_df["status"].isin(clear_statuses)].copy()
    pgsic_dict, pg_dict = create_civdef_dict(clear_df)

    imputed_df = apply_civdev_imputation(filtered_df, pgsic_dict, pg_dict)
    final_df = tmi.apply_to_original(imputed_df, df)

    return final_df
