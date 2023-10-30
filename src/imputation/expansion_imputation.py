"""Expansion imputation for the 2xx and 3xx questions.
"""

import logging
import pandas as pd
from typing import List, Union

from src.utils.wrappers import time_logger_wrap

ExpansionLogger = logging.getLogger(__name__)


def evaluate_imputed_ixx(
    group: pd.core.groupby.DataFrameGroupBy,
    master_col: str,
    break_down_cols: List[Union[str, int]],
) -> pd.DataFrame:
    """Evaluate the imputed 2xx or 3xx as the sum of all 2xx or 3xx
    over the sum of all 211 or 305 values, multiplied by the imputed 211.
    """
    # Make cols into str just in case coming through as ints
    bd_cols = [str(col) for col in break_down_cols]

    # Sum the master column, e.g. "211" or "305" for the clear responders
    clear_statuses = ["Clear", "Clear - overridden"]
    clear_mask = group["status"].isin(clear_statuses)
    sum_master_q = group.loc[clear_mask][master_col].sum()  # scalar

    # Calculate the imputation columns for the breakdown questions
    for bd_col in bd_cols:
        # Sum the breakdown q for the (clear) responders
        sum_breakdown_q = group.loc[clear_mask][bd_col].sum()

        # Master col imputed, e.g. 211_imputed or 305_imputed
        # Sum the breakdown master q imputed for TMI records
        TMI_mask = group["imp_marker"] == "TMI"
        master_imp_val = group.loc[TMI_mask][f"{master_col}_imputed"]
        if master_imp_val.values.any():
            master_imp_val = master_imp_val.values[0]  # get a single value
        else:
            master_imp_val = float("nan")  # assign nan where there are no vals

        # Make imputation col equal to original column
        group[f"{bd_col}_imputed"] = group[bd_col]

        # Update the imputation column for status encoded 100 and 201
        # i.e. for non-responders
        unclear_statuses = ["Form sent out", "Check needed"]
        unclear_mask = group["status"].isin(unclear_statuses)
        imputed_value = (sum_breakdown_q / sum_master_q) * master_imp_val
        # Write imputed value to the non-responder records
        group.loc[unclear_mask, f"{bd_col}_imputed"] = imputed_value

    # Returning updated group and updated QA dict
    return group


def split_df_on_trim(df: pd.DataFrame, trim_bool_col: str) -> pd.DataFrame:
    """Splits the dataframe in based on if it was trimmed or not"""

    df_not_trimmed = df.loc[~df[trim_bool_col]]
    df_trimmed = df.loc[df[trim_bool_col]]

    return df_trimmed, df_not_trimmed


@time_logger_wrap
def run_expansion(df: pd.DataFrame, config: dict):
    """The main 'entry point' function to run the expansion imputation."""

    # Step 4: Expansion imputation for breakdown questions
    # TODO: remove this temporary fix to cast Nans to False
    df["211_trim"].fillna(False, inplace=True)
    df["305_trim"].fillna(False, inplace=True)

    # Filter to exclude the same rows trimmed for 211_trim == False
    trimmed_211_df, nontrimmed_df = split_df_on_trim(df, "211_trim")
    ExpansionLogger.debug(
        f"There are {df.shape[0]} rows in the original df \n"
        f"There are {nontrimmed_df.shape[0]} rows in the nontrimmed_df \n"
        f"There are {trimmed_211_df.shape[0]} rows in the trimmed_211_df"
    )

    # Trimmed groups
    non_trim_grouped = nontrimmed_df.groupby("imp_class")

    # Calculate the imputation values for 2xx questions
    breakdown_qs_2xx = config["breakdowns"]["2xx"]
    result_211_df = non_trim_grouped.apply(
        evaluate_imputed_ixx, "211", break_down_cols=breakdown_qs_2xx
    )

    # Join the 211 expanded df (processed from untrimmed records) back on to
    # 211 trimmed records
    result_211_df = pd.concat([result_211_df, trimmed_211_df], axis=0)

    # Calculate the imputation values for 3xx questions
    breakdown_qs_3xx = config["breakdowns"]["3xx"]

    # TODO: Fix datatypes and remove this temp-fix
    result_211_df.loc[:, breakdown_qs_3xx] = result_211_df.loc[
        :, breakdown_qs_3xx
    ].fillna(0)
    result_211_df.loc[:, breakdown_qs_3xx] = result_211_df.loc[
        :, breakdown_qs_3xx
    ].astype(int)

    # Filter to exclude the same rows trimmed for 305_trim == False
    trimmed_305_df, nontrimmed_df = split_df_on_trim(result_211_df, "305_trim")
    ExpansionLogger.debug(
        f"There are {result_211_df.shape[0]} rows in the result_211_df \n"
        f"There are {nontrimmed_df.shape[0]} rows in the nontrimmed_df"
    )

    # Groupby imp_class again
    non_trim_grouped = nontrimmed_df.groupby("imp_class")

    result_211_305_df = non_trim_grouped.apply(
        evaluate_imputed_ixx, "305", break_down_cols=breakdown_qs_3xx
    )

    # Join the expanded df (processed from untrimmed records) back on to
    # trimmed records
    expanded_result_df = pd.concat([result_211_305_df, trimmed_305_df], axis=0)
    expanded_result_df = expanded_result_df.sort_values(
        ["reference", "instance"], ascending=[True, True]
    ).reset_index(drop=True)

    return expanded_result_df
