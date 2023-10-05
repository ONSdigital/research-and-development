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

    # Filter out imputation classes that are missing either "201" or "rusic"
    # and exclude empty pg_sic classes
    cond1 = ~(df["pg_sic_class"].str.contains("nan")) & (df["empty_group"] == False)
    filtered_df = df[cond1]

    # Group by the pg_sic imputation class the loop through the groups
    pg_sic_grp = filtered_df.loc[filtered_df["instance"] != 0].groupby("pg_sic_class")

    for pg_sic_class, class_group in pg_sic_grp:
        civdef_dict[pg_sic_class] = calc_cd_proportions(class_group)

    # create a second dictionary to hold civil or defence ratios 
    # for the "empty_group" cases
    civdef_empty_group_dict = {}

    cond2 = ~(df["pg_class"].str.contains("nan")) 
    filtered_df2 = df[cond2]

    # Group by the pg-only imputation class the loop through the groups
    pg_grp = filtered_df2.loc[filtered_df2["instance"] != 0].groupby("pg_class")

    # for pg_class, class_group in pg_grp:
    #     # check if any item in the class has "empty group"
    #    # class_group["num_empty"] = 
    #     civdef_empty_group_dict[pg_class] = calc_cd_proportions(class_group)

    return civdef_dict, df


def prep_cd_imp_classes(df):
    # create imputation classes based on product group and rusic
    df = tmi.create_imp_class_col(df, "201", "rusic", "pg_sic_class")

    clear_status_cond = df["statusencoded"].isin(["210", "211"])
    df["valid_civdef_val"] = ~df["200"].isnull() & clear_status_cond

    # calculate the number of valid entries in the class for column 200 
    num_valid = df.groupby("pg_sic_class")["valid_civdef_val"].transform(sum)

    # exclude classes that do not contain are not valid
    valid_class_cond = ~(df["pg_sic_class"].str.contains("nan"))

    df["empty_group"] = valid_class_cond &  (num_valid == 0) 

    # create a new imputation class called "pg_class" to be used only when
    # "empty_group" is true (ie, no avaible R&D type entries)
    # this imputation class will include product group (col 201) only.
    df = tmi.create_imp_class_col(df, "201", None, "pg_class")

    df = df.drop("valid_civdef_val", axis=1)

    return df


def apply_civdev_imputation(
    df: pd.DataFrame, 
    cd_dict: Dict[str, float]
) -> pd.DataFrame:
    """Apply imputation for R&D type for non-responders and 'No R&D'.

    Values in column 200 (R&D type) are imputed with either "C" for civl or
    "D" for defence, based on ratios in the same imputation class in 
    clear responders.
    
    Args:
        df (pd.DataFrame): The dataframe of all responses
        cd_dict (Dict[str, Tuple(float, float)]): Dictionary with values to 
            use for imputation.

    Returns:
        pd.DataFrame: An updated dataframe with new col "200_imputed".
    '"""
    df["200_imputed"] = "N/A"
    df = df.copy()
    # filter out empty and invalid imputation classes
    cond1 = (df["empty_group"] == False) & ~(df["pg_sic_class"].str.contains("nan"))
    # filter for 'No R&D' and for status 'Form sent out'
    cond2 = (df["status"] == "Form sent out") | (df["604"] == "No")

    # also exclude instance 0


    filtered_df = df.loc[cond1 & cond2]

    # use groupby to update each class in turn.

    # create new column
    filtered_df["200_imputed"] = "Test"
    updated_df = tmi.apply_to_original(filtered_df, df)

    return df


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

    df = prep_cd_imp_classes(df)

    # Filter for clear statuses
    clear_df = tmi.filter_by_column_content(df, "statusencoded", ["210", "211"])

    # exclude empty pg_rusic imputation classes
  #  clear_df = clear_df.loc[df["empty_group"] == False]

    # create a dict mapping each class to either 'C' (civil) or 'D'(defence)
    civil_def_dict, clear_df = create_civdef_dict(clear_df)

    df = apply_civdev_imputation(df, civil_def_dict)

    return df