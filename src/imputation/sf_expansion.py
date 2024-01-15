"""Module containing all functions relating to short form expansion."""
from typing import List, Union
import pandas as pd
import logging

from src.imputation.imputation_helpers import split_df_on_imp_class
from src.utils.wrappers import df_change_func_wrap

SFExpansionLogger = logging.getLogger(__name__)

formtype_long = "0001"
formtype_short = "0006"


def expansion_impute(
    group: pd.core.groupby.DataFrameGroupBy,
    master_col: str,
    trim_col: str,
    group_type: str,
    threshold_num: int,
    break_down_cols: List[Union[str, int]],
) -> pd.DataFrame:
    """Calculate the expansion imputated values for short forms using long form data"""
    group_copy = group.copy()

    imp_class = group_copy["imp_class"].values[0]

    # Make cols into str just in case coming through as ints
    bd_cols = [str(col) for col in break_down_cols]

    # Make long and short masks
    long_mask = group_copy["formtype"] == formtype_long
    short_mask = group_copy["formtype"] == formtype_short

    # Create mask for clear responders
    clear_statuses = ["Clear", "Clear - overridden"]
    clear_mask = group_copy["status"].isin(clear_statuses)

    # Create a mask to exclude trimmed values
    exclude_trim_mask = group_copy[trim_col].isin([False])

    # Masks to select correct records for summing
    long_responder_mask = clear_mask & long_mask & exclude_trim_mask
    to_expand_mask = short_mask

    # Condition for positive values in the master column
    pos_cond = group_copy[master_col] > 0
    # Calculate the number of non-zero long-form clear responders in the master column
    threshold_check = len(group_copy.loc[(long_responder_mask & pos_cond), master_col])

    # If there are fewer than "threshold_num" non-zero clear responders in the
    # imputation class then do not attempt to calculate the breakdowns at the imputation
    # class level. In this case the values previously calculated in the
    # "civil defence fallback" group will be used instead.

    if (group_type == "imp_class_group") & (threshold_check <= threshold_num):
        SFExpansionLogger.debug(
            f"Imputation class: {imp_class} has fewer than {threshold_num} "
            "clear responders."
        )
        return group_copy

    # Get long forms only for summing the master_col (scalar value)
    sum_master_q_lng = group_copy.loc[long_responder_mask, master_col].sum()

    # We calculate breakdown values for both responder and imputed short form rows
    # Get the master (e.g. 211) value for all short form rows (will be a vector)
    # Note: the _imputed columns contain both original and imputed values.
    master_col_imputed = f"{master_col}_imputed"
    returned_master_vals = group_copy.loc[to_expand_mask, master_col_imputed]

    # Calculate the imputation columns for the breakdown questions
    for bd_col in bd_cols:
        # Sum the breakdown q for the (clear) responders
        sum_breakdown_q = group_copy.loc[long_responder_mask, bd_col].sum()

        # Calculate the values of the imputation column
        if sum_master_q_lng > 0:
            scale_factor = sum_breakdown_q / sum_master_q_lng
        else:
            scale_factor = 0

        imputed_sf_vals = scale_factor * returned_master_vals

        # Write imputed value to all records
        group_copy.loc[short_mask, f"{bd_col}_imputed"] = imputed_sf_vals

    # Indicate how the short_form expansion has been computed, whether with the
    # civil and defence fallback, or by imputation class.
    group_copy[f"{master_col}_sf_exp_grouping"] = group_type

    return group_copy


# @df_change_func_wrap
def apply_expansion(
    df: pd.DataFrame,
    master_values: List,
    breakdown_dict: dict,
    threshold_num: int = 3,
):

    # Renaming this df to use in the for loop
    expanded_df = df.copy()

    # Cast nulls in the boolean trim columns to False
    expanded_df[["211_trim", "305_trim"]] = expanded_df[
        ["211_trim", "305_trim"]
    ].fillna(False)

    for master_value in master_values:
        # exclude the "305" case which will be based on different trimming
        if master_value == "305":
            trim_col = "305_trim"
        else:
            trim_col = "211_trim"

        SFExpansionLogger.debug(f"Processing exansion imputation for {master_value}")

        # for a first pass, group by civil or defence only
        groupby_obj_cd = expanded_df.groupby("200")

        # Calculate the imputation values for master question
        expanded_df = groupby_obj_cd.apply(
            expansion_impute,
            master_value,
            trim_col,
            "civil_defence_fallback",
            threshold_num,
            break_down_cols=breakdown_dict[master_value],
        )  # returns a dataframe

        expanded_df.reset_index(drop=True, inplace=True)

        # For the second pass, group by imputation class
        groupby_obj_impcl = expanded_df.groupby("imp_class")

        # Calculate the imputation values for master question
        expanded_df = groupby_obj_impcl.apply(
            expansion_impute,
            master_value,
            trim_col,
            "imp_class_group",
            threshold_num,
            break_down_cols=breakdown_dict[master_value],
        )  # returns a dataframe

        expanded_df.reset_index(drop=True, inplace=True)

    # Calculate the headcount_m and headcount_f imputed values by summing
    short_mask = expanded_df["formtype"] == formtype_short

    expanded_df.loc[short_mask, "headcount_tot_m_imputed"] = (
        expanded_df["headcount_res_m_imputed"]
        + expanded_df["headcount_tec_m_imputed"]
        + expanded_df["headcount_oth_m_imputed"]
    )

    expanded_df.loc[short_mask, "headcount_tot_f_imputed"] = (
        expanded_df["headcount_res_f_imputed"]
        + expanded_df["headcount_tec_f_imputed"]
        + expanded_df["headcount_oth_f_imputed"]
    )

    return expanded_df


def prepare_short_form_constructed(df: pd.DataFrame, master_cols: List) -> pd.DataFrame:
    """Prepare the constructed short form responses for sf expansion.
    
    The constructed records were removed from imputation, so it is necessary to copy
    the master columns to the empty "imputed" master columns.
    It is also necessary to create imputation classes for these records.

    For example, column "211" needs to be copied to "211_imputed" in these cases.
    """
    sf_constructed_mask = (df.formtype == "0006") & (df.imp_marker == "constructed")

    # Create imputation class for the short
    df.loc[sf_constructed_mask, "imp_class"] = (
        df.loc[sf_constructed_mask, "200"] + df.loc[sf_constructed_mask, "201"]
    )

    # Copy the values of the master columns to the corresponding "_imputed" column
    for col in master_cols:
        df.loc[sf_constructed_mask, f"{col}_imputed"] = df.loc[sf_constructed_mask, col]
    
    return df


@df_change_func_wrap
def run_sf_expansion(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Calculate the expansion imputated values for short forms using long form data."""
    # Get dictionary of short form master keys (or target variables)
    # and breakdown variables
    breakdown_dict = config["breakdowns"]
    master_values = list(breakdown_dict)

    # Prepare constructed short-form entries for sf expansion imputation
    df = prepare_short_form_constructed(df, master_values)

    # Remove records that have the reference list variables
    # and those that have "nan" in the imp class
    filtered_df, excluded_df = split_df_on_imp_class(df)

    # Obtain the "threshold_num" from the config
    # (this is the minimum viable number in an imputation class)
    threshold_num = config["imputation"]["sf_expansion_threshold"]

    # Run the `expansion_impute` function in a for-loop via `apply_expansion`
    expanded_df = apply_expansion(
        filtered_df,
        master_values,
        breakdown_dict,
        threshold_num,
    )

    # Re-include those records from the reference list before returning df
    result_df = pd.concat([expanded_df, excluded_df], axis=0)

    result_df = result_df.sort_values(
        ["reference", "instance"], ascending=[True, True]
    ).reset_index(drop=True)

    SFExpansionLogger.info("Short-form expansion imputation completed.")

    return result_df
