"""Module containing all functions relating to short form expansion.
"""

from typing import List, Union
import pandas as pd
import logging

from src.imputation.expansion_imputation import split_df_on_trim
from src.imputation.tmi_imputation import create_imp_class_col, apply_to_original
from src.utils.wrappers import df_change_func_wrap


SFExpansionLogger = logging.getLogger(__name__)

formtype_long = "0001"
formtype_short = "0006"


def expansion_impute(
    group: pd.core.groupby.DataFrameGroupBy,
    master_col: str,
    break_down_cols: List[Union[str, int]],
) -> pd.DataFrame:
    """Calculate the expansion imputated values for short forms using long form data"""

    # Create a copy of the group dataframe
    group_copy = group.copy()

    imp_class = group_copy["imp_class"].values[0]
    SFExpansionLogger.debug(f"Imputation class: {imp_class}.")
    SFExpansionLogger.debug(f"Master column: {master_col}.")

    # Make cols into str just in case coming through as ints
    bd_cols = [str(col) for col in break_down_cols]

    # Make long and short masks
    long_mask = group_copy["formtype"] == formtype_long
    short_mask = group_copy["formtype"] == formtype_short

    # Create mask for clear responders
    clear_statuses = ["Clear", "Clear - overridden"]
    clear_mask = group_copy["status"].isin(clear_statuses)

    # Combination masks to select correct records for summing
    # NOTE: we only use clear responders in calculations
    long_responder_mask = clear_mask & long_mask
    short_responder_mask = clear_mask & short_mask

    # Get long forms only for summing master_col (scalar value)
    sum_master_q_lng = group_copy.loc[long_responder_mask, master_col].sum()

    # Get the master (e.g. 211) returned value for each responder (will be a vector)
    returned_master_vals = group_copy[short_responder_mask][master_col]

    # Calculate the imputation columns for the breakdown questions
    for bd_col in bd_cols:
        # Sum the breakdown q for the (clear) responders
        sum_breakdown_q = group_copy.loc[long_responder_mask, bd_col].sum()

        # Update the imputation column for status encoded 100 and 201
        # i.e. for non-responders
        imputed_sf_vals = (sum_breakdown_q / sum_master_q_lng) * returned_master_vals

        # Write imputed value to all records
        group_copy.loc[short_mask, f"{bd_col}_imputed"] = imputed_sf_vals

    # Returning updated group and updated QA dict
    return group_copy


# @df_change_func_wrap
def apply_expansion(df: pd.DataFrame, master_values: List, breakdown_dict: dict):

    df = df.copy()

    # Filter to exclude the 211 trimming, the excluded rows are in trimmed_211_df
    # and the dataframe after trimming in nontrimmed_df
    trimmed_211_df, nontrimmed_df = split_df_on_trim(df, "211_trim")
    SFExpansionLogger.debug(
        f"There are {df.shape[0]} rows in the original df \n"
        f"There are {nontrimmed_df.shape[0]} rows in the nontrimmed_df \n"
        f"There are {trimmed_211_df.shape[0]} rows in the trimmed_211_df"
    )
    # Renaming this df to use in the for loop
    expanded_df = nontrimmed_df

    for master_value in master_values:
        # exclude the "305" case which will be based on different trimming
        if master_value == "305":
            continue

        SFExpansionLogger.debug(f"Processing exansion imputation for {master_value}")

        # Create group_by obj of the trimmed df
        non_trim_grouped = expanded_df.groupby("imp_class")  # groupby object

        # Calculate the imputation values for master question
        expanded_df = non_trim_grouped.apply(
            expansion_impute,
            master_value,
            break_down_cols=breakdown_dict[master_value],
        )  # returns a dataframe

    # Concat the expanded df (processed from untrimmed records) back on to
    # trimmed records. Reassigning to `df` to feed back into for-loop
    combined_df = pd.concat([expanded_df, trimmed_211_df], axis=0)

    # Set master value to be "305"
    master_value = "305"

    # now filter on the 305 trimming- the excluded rows are in trimmed_305_df
    # and the dataframe after trimming in nontrimmed_df
    trimmed_305_df, nontrimmed_df = split_df_on_trim(combined_df, "305_trim")
    SFExpansionLogger.debug(
        f"There are {df.shape[0]} rows in the original df \n"
        f"There are {nontrimmed_df.shape[0]} rows in the nontrimmed_df \n"
        f"There are {trimmed_305_df.shape[0]} rows in the trimmed_305_df"
    )

    SFExpansionLogger.debug(f"Processing expansion imputation for {master_value}")

    # Create group_by obj of the trimmed df
    non_trim_grouped = nontrimmed_df.groupby("imp_class")

    # Calculate the imputation values for master question
    expanded_305_df = non_trim_grouped.apply(
        expansion_impute,
        master_value,
        break_down_cols=breakdown_dict[master_value],
    )
    # Concat the expanded df (processed from untrimmed records) back on to
    # trimmed records. Reassigning to `df` to feed back into for-loop
    combined_df = pd.concat([expanded_305_df, trimmed_305_df], axis=0)

    # Calculate the headcount_m and headcount_f imputed values by summing
    short_mask = combined_df["formtype"] == formtype_short

    combined_df.loc[short_mask, "headcount_tot_m_imputed"] = (
        combined_df["headcount_res_m_imputed"]
        + combined_df["headcount_tec_m_imputed"]
        + combined_df["headcount_oth_m_imputed"]
    )

    combined_df.loc[short_mask, "headcount_tot_f_imputed"] = (
        combined_df["headcount_res_f_imputed"]
        + combined_df["headcount_tec_f_imputed"]
        + combined_df["headcount_oth_f_imputed"]
    )

    return combined_df


# @df_change_func_wrap
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


@df_change_func_wrap
def run_sf_expansion(df: pd.DataFrame, config: dict) -> pd.DataFrame:

    # Get the breakdowns dict
    breakdown_dict = config["breakdowns"]

    # TODO: Move this imp_class step to census TMI
    short_form_df = df.loc[(df["formtype"] == "0006") & (df["instance"] != 0)]
    short_form_df = create_imp_class_col(short_form_df, "200", "201", "imp_class")

    # Re-joining the output of create_imp_class_col to original df
    df = apply_to_original(short_form_df, df)

    # Remove records that have the reference list variables
    # and those that have "nan" in the imp class
    filtered_df, excluded_df = split_df_on_imp_class(df)

    # Get master keys
    master_values = breakdown_dict.keys()

    # Run the `expansion_impute` function in a for-loop via `apply_expansion`
    expanded_df = apply_expansion(filtered_df, master_values, breakdown_dict)

    # Re-include those records from the reference list before returning df
    result_df = pd.concat([expanded_df, excluded_df], axis=0)

    result_df = result_df.sort_values(
        ["reference", "instance"], ascending=[True, True]
    ).reset_index(drop=True)

    SFExpansionLogger.info("Short-form expansion imputation completed.")

    return result_df
