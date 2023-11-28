"""Utility functions  to be used in the imputation module."""
from typing import List
import pandas as pd



def instance_fix(df: pd.DataFrame):
    """Set instance to 1 for longforms with status 'Form sent out.'

    References with status 'Form sent out' initially have a null in the instance
    column.
    """
    mask = (df.formtype == "0001") & (df.status == "Form sent out")
    df.loc[mask, "instance"] = 1
 
    return df


def duplicate_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Create a duplicate long form records with no R&D and set instance to 1.
    
    These references initailly have one entry with instance 0. 
    A copy will be created with instance set to 1.
    """
    mask = (df.formtype == "0001") & (df["604"] == "No")
    filtered_df = df.copy().loc[mask]
    filtered_df["instance"] == 1

    updated_df = pd.concat([df, filtered_df], ignore_index=True)
    updated_df = updated_df.sort_values(
        ["reference", "instance"], ascending=[True, True]
    ).reset_index(drop=True)
    return updated_df


def split_df_on_trim(df: pd.DataFrame, trim_bool_col: str) -> pd.DataFrame:
    """Splits the dataframe in based on if it was trimmed or not"""

    # TODO: remove this temporary fix to cast Nans to False
    df[trim_bool_col].fillna(False, inplace=True)

    df_not_trimmed = df.loc[~df[trim_bool_col]]
    df_trimmed = df.loc[df[trim_bool_col]]

    return df_trimmed, df_not_trimmed


def split_df_on_imp_class(df: pd.DataFrame, exclusion_list: List = ["817", "nan"]):

    # Exclude the records from the reference list
    exclusion_str = "|".join(exclusion_list)

    # Create the filter
    exclusion_filter = df["imp_class"].str.contains(exclusion_str)
    # Where imputation class is null, `NaN` is returned by the
    # .str.contains(exclusion_str) so we need to swap out the
    # returned `NaN`s with True, so it gets filtered out
    exclusion_filter = exclusion_filter.fillna(True)

    # Filter out imputation classes that include "817" or "nan"
    filtered_df = df[~exclusion_filter]  # df has 817 and nan filtered out
    excluded_df = df[exclusion_filter]  # df only has 817 and nan records

    return filtered_df, excluded_df
