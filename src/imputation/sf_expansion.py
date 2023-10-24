"""Module containing all functions relating to short form expansion.
"""

from typing import List, Union
import pandas as pd
import logging

from src.imputation.expansion_imputation import split_df_on_trim

SFExpansionLogger = logging.getLogger(__name__)

formtype_long = "0001"
formtype_short = "0006"


def expansion_impute(
    group: pd.core.groupby.DataFrameGroupBy,
    master_col: str,
    break_down_cols: List[Union[str, int]],
    long_code=formtype_long,
    short_code=formtype_short,
) -> pd.DataFrame:
    """Calculate the expansion imputated values for short forms using long form data"""

    imp_class = group["imp_class"].values[0]
    SFExpansionLogger.debug(f"Imputation class: {imp_class}.")
    SFExpansionLogger.debug(f"Master column: {master_col}.")

    # Make cols into str just in case coming through as ints
    bd_cols = [str(col) for col in break_down_cols]

    # Make long and short masks
    long_mask = group["formtype"] == long_code
    short_mask = group["formtype"] == short_code

    # Sum the master column, e.g. "211" or "305" for the clear responders
    clear_statuses = ["Clear", "Clear - overridden"]
    responder_mask = group["status"].isin(clear_statuses) | group["imp_marker"] == "TMI"

    # Combination masks to select correct records for summing
    long_form_responder_mask = responder_mask & long_mask
    short_form_responder_mask = responder_mask & short_mask

    # Get long forms only for summing master_col
    sum_master_q_lng = group.loc[long_form_responder_mask, master_col].sum()  # scalar

    # Get the master (e.g. 211) returned value for each responder (will be a vector)
    returned_master_vals = group[short_form_responder_mask][master_col]  # vector

    # Calculate the imputation columns for the breakdown questions
    for bd_col in bd_cols:
        # Sum the breakdown q for the (clear) responders
        sum_breakdown_q = group.loc[long_form_responder_mask][bd_col].sum()

        # Make imputation col equal to original column
        group[f"{bd_col}_imputed"] = group[bd_col]

        # Update the imputation column for status encoded 100 and 201
        # i.e. for non-responders
        imputed_sf_vals = (sum_breakdown_q / sum_master_q_lng) * returned_master_vals
        # Write imputed value to the non-responder records
        group.loc[short_form_responder_mask, f"{bd_col}_imputed"] = imputed_sf_vals

    # Returning updated group and updated QA dict
    return group


def run_sf_expansion(df: pd.DataFrame, config: dict) -> pd.DataFrame:

    # Get the breakdowns dict
    breakdown_dict = config["breakdowns"]

    # Exclude the records from the reference list
    refence_list = ["817"]
    ref_list_excluded_df = df[~df.cellnumber.isin(refence_list)]
    ref_list_only_df = df[df.cellnumber.isin(refence_list)]

    # Get master keys
    master_values = breakdown_dict.keys()

    for master_value in master_values:
        SFExpansionLogger.debug(f"Processing exansion imputation for {master_value}")
        # Filter to exclude the same rows trimmed for 211_trim == False
        trimmed_df, nontrimmed_df = split_df_on_trim(
            ref_list_excluded_df, f"{master_value}_trim"
        )
        SFExpansionLogger.debug(
            f"There are {df.shape[0]} rows in the original df \n"
            f"There are {nontrimmed_df.shape[0]} rows in the nontrimmed_df \n"
            f"There are {trimmed_df.shape[0]} rows in the {master_value} trimmed_df"
        )

        # Create group_by obj of the trimmed df
        non_trim_grouped = nontrimmed_df.groupby("imp_class")

        # Calculate the imputation values for master question
        expanded_df = non_trim_grouped.apply(
            expansion_impute,
            master_value,
            break_down_cols=breakdown_dict[master_value],
        )

        # Concat the expanded df (processed from untrimmed records) back on to
        # trimmed records
        result_df = pd.concat([expanded_df, trimmed_df], axis=0)

    # Re-include those records from the reference list before returning df
    result_df = pd.concat([result_df, ref_list_only_df], axis=0)

    return result_df
