"""Module containing all functions relating to short form expansion.
"""

from typing import List, Union
import pandas as pd
import logging

SFExpansionLogger = logging.getLogger(__name__)

formtype_long = "0001"
formtype_short = "0006"


def expansion_impute(
    group: pd.core.groupby.DataFrameGroupBy,
    master_col: str,
    break_down_cols: List[Union[str, int]],
    formtype_lng,
    formtype_shrt,
) -> pd.DataFrame:
    """Calculate the expansion imputated values for short forms using long form data"""

    imp_class = group["imp_class"].values[0]
    SFExpansionLogger.debug(f"Imputation class: {imp_class}")

    # Make cols into str just in case coming through as ints
    bd_cols = [str(col) for col in break_down_cols]

    # Make long and short masks
    long_mask = group["formtype"] == formtype_lng
    short_mask = group["formtype"] == formtype_shrt

    # Sum the master column, e.g. "211" or "305" for the clear responders
    clear_statuses = ["Clear", "Clear - overridden"]
    responder_mask = group["status"].isin(clear_statuses)

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

    # Groupby imputation class
    grouped_by_impclass = ref_list_excluded_df.groupby("imp_class")
    print(grouped_by_impclass)

    # Set up a list to grab the dataframes
    impute_df_lst = []

    # Get master keys
    master_keys = breakdown_dict.keys()
    print(master_keys)

    # for master_key in master_keys:
    #     breakdown_qs = breakdown_dict[master_key]

    #     imputed_df = grouped_by_impclass.apply(
    #     expansion_impute,
    #     master_key,
    #     breakdown_qs,
    #     formtype_long,
    #     formtype_short
    # )
    #     impute_df_lst.append(imputed_df)

    # 211 trim - split

    # concat back on trimmed records

    # 305 trim - split

    # concat back on trimmed records

    # emp_total headcount_total no trimming

    # Combine imputed dataframes, dropping dupe columns
    imputed_df = pd.concat(impute_df_lst, axis=1)

    # Drop columns
    imputed_df = imputed_df.loc[:, ~imputed_df.columns.duplicated()]

    return imputed_df
