"""Expansion imputation for the 2xx and 3xx questions.
"""

import logging
import pandas as pd
from typing import List, Union

from src.imputation.imputation_helpers import split_df_on_imp_class, split_df_on_trim

ExpansionLogger = logging.getLogger(__name__)


def evaluate_imputed_ixx(
    group: pd.core.groupby.DataFrameGroupBy,
    master_col: str,
    break_down_cols: List[Union[str, int]],
) -> pd.DataFrame:
    """Evaluate the imputed 2xx or 3xx as the sum of all 2xx or 3xx
    over the sum of all 211 or 305 values, multiplied by the imputed 211.
    """
    # Return the groupby object unaltered if there are no TMI values
    # in the imputation class
    TMI_mask = group["imp_marker"] == "TMI"
    imputed_subgroup = group.loc[TMI_mask]

    if imputed_subgroup.empty:
        return group

    # Make cols into str just in case coming through as ints
    bd_cols = [str(col) for col in break_down_cols]

    # Sum the master column, e.g. "211" or "305" for the clear responders
    clear_statuses = ["Clear", "Clear - overridden"]
    clear_mask = group["status"].isin(clear_statuses)
    sum_master_q = group.loc[clear_mask, master_col].sum()  # scalar

    # Find the imputed value for the master col for the current imputation class
    # As this value is the same for the whole imputation class, we return the first
    master_imp_val = group.loc[TMI_mask, f"{master_col}_imputed"].values[0]  # scalar

    # Calculate the value to enter as the imputed value for the breakdown cols
    for bd_col in bd_cols:
        # Sum the breakdown q for the (clear) responders
        sum_breakdown_q = group.loc[clear_mask, bd_col].sum()

        # calculate the imputed values
        if master_imp_val > 0:
            imputed_value = (sum_breakdown_q / sum_master_q) * master_imp_val
        else:
            imputed_value = 0

        # Update the imputed break-down columns where TMI imputation has been applied
        group.loc[TMI_mask, f"{bd_col}_imputed"] = imputed_value

    # Returning updated group and updated QA dict
    return group


def run_expansion(df: pd.DataFrame, config: dict):
    """The main 'entry point' function to run the expansion imputation.

    Expansion imputation is Step 4 in TMI imputation for longforms, and
    calculates imputed values for breakdown questions 2xx and 3xx
    for the rows where TMI has been applied.

    To calculate the values to use in imputation, clear longform responders
    are used, and the imputed values are applied to rows where imp_marker = TMI.

    Args:
        df (pd.DataFrame): the full dataframe after TMI imputation
        config (Dict): the configuration settings
    Returns:
        pd.DataFrame: the full dataframe with imputed 2xx and 3xx columns.
    """
    # Get the breakdowns dict
    breakdown_dict = config["breakdowns"]
    breakdown_qs_2xx = breakdown_dict["211"]
    breakdown_qs_3xx = breakdown_dict["305"]

    # Remove records that have the reference list variables
    # and those that have "nan" in the imp class
    filtered_df, excluded_df = split_df_on_imp_class(df, ["nan"])

    # Filter to exclude the same rows trimmed for 211_trim == False
    trimmed_211_df, nontrimmed_df = split_df_on_trim(filtered_df, "211_trim")

    # Trimmed groups
    non_trim_grouped = nontrimmed_df.groupby("imp_class", group_keys=False)

    # Calculate the imputation values for 2xx questions
    result_211_df = non_trim_grouped.apply(
        evaluate_imputed_ixx, "211", break_down_cols=breakdown_qs_2xx
    )

    # Join the 211 expanded df (processed from untrimmed records) back on to
    # 211 trimmed records
    result_211_df = pd.concat([result_211_df, trimmed_211_df], axis=0)

    # Calculate the imputation values for 3xx questions

    # Filter to exclude the same rows trimmed for 305_trim == False
    trimmed_305_df, nontrimmed_df = split_df_on_trim(result_211_df, "305_trim")

    # Groupby imp_class again
    non_trim_grouped = nontrimmed_df.groupby("imp_class", group_keys=False)

    result_211_305_df = non_trim_grouped.apply(
        evaluate_imputed_ixx, "305", break_down_cols=breakdown_qs_3xx
    )

    # Join the expanded df (processed from untrimmed records) back on to
    # trimmed records for 305 trimming, and the dataframe of excluded rows
    expanded_result_df = pd.concat(
        [result_211_305_df, trimmed_305_df, excluded_df], axis=0
    )
    expanded_result_df = expanded_result_df.sort_values(
        ["reference", "instance"], ascending=[True, True]
    ).reset_index(drop=True)

    return expanded_result_df
