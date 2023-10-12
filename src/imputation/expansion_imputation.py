"""Expansion imputation for the 2xx and 3xx questions.
"""

import logging
import pandas as pd

from typing import List, Union

from src.utils.wrappers import df_change_func_wrap, time_logger_wrap

ExpansionLogger = logging.getLogger(__name__)


@time_logger_wrap
def evaluate_imputed_ixx(
    group: pd.core.groupby.DataFrameGroupBy,
    master_col: str,
    break_down_cols: List[Union[str, int]],
) -> pd.DataFrame:
    """Evaluate the imputed 2xx or 3xx as the sum of all 2xx or 3xx
    over the sum of all 211 or 305 values, multiplied by the imputed 211."""

    # Make cols into str just in case coming through as ints
    bd_cols = [str(col) for col in break_down_cols]

    # Make list of imputed column names
    imp_cols = [f"{col}_imputed" for col in bd_cols]

    # Sum the master column, e.g. "211" or "305" for the clear responders
    clear_statuses = ["210", "211"]
    sum_master_q = group.loc[group["statusencoded"].isin(clear_statuses)][
        master_col
    ].sum()  # scalar

    # Breakdown questions summed
    # bds_summed = group[bd_cols].sum(axis=0)  # vector

    # # Master col imputed, e.g. 211_imputed or 305_imputed
    # master_col_imputed = group[f"{master_col}_imputed"].values  # vector

    # Calculate the imputation columns for the breakdown questions
    # group[imp_cols] = ((bds_summed.values / sum_master_q) * master_col_imputed).values
    print(f"Group: {group['imp_class'].values[0]}")
    for imp_col, bd_col in zip(imp_cols, bd_cols):
        # Sum the breakdown q for the responders
        clear_statuses = ["210", "211"]
        clear_mask = group["statusencoded"].isin(clear_statuses)
        sum_breakdown_q = group.loc[clear_mask][bd_col].sum()

        # Sum the breakdown master q imputed for TMI records
        TMI_mask = group["imp_marker"] == "TMI"
        sum_master_imp = group.loc[TMI_mask][f"{master_col}_imputed"]
        sum_master_imp = sum_master_imp.values[0]  # get a single value

        # Update the imputation column for status encoded 100 and 201
        unclear_statuses = ["100", "201"]
        unclear_mask = group["statusencoded"].isin(unclear_statuses)
        group.loc[unclear_mask][imp_col] = (
            sum_breakdown_q / sum_master_q
        ) * sum_master_imp

    return group


@df_change_func_wrap
def only_trimmed_records(df: pd.DataFrame, trim_bool_col: str) -> pd.DataFrame:
    """Trims the dataframe to only include records that have False in the
    trim_bool_col column"""

    return df.loc[~df[trim_bool_col]]


def run_expansion(df: pd.DataFrame, config: dict):
    """The main 'entry point' function to run the expansion imputation."""

    # Step 4: Expansion imputation for breakdown questions

    # TODO: remove this temporary fix to cast Nans to False
    df["211_trim"].fillna(False, inplace=True)

    # Filter to exclude the same rows trimmed for 211_trim == False
    trimmed_df = only_trimmed_records(df, "211_trim")
    ExpansionLogger.debug(f"There are {trimmed_df.shape[0]} rows in the trimmed_df")

    # Trimmed groups
    trim_grouped = trimmed_df.groupby("imp_class")

    # Calculate the imputation values for 2xx questions
    breakdown_qs_2xx = config["breakdowns"]["2xx"]
    result_211_df = trim_grouped.apply(
        evaluate_imputed_ixx, "211", break_down_cols=breakdown_qs_2xx
    )

    # Calculate the imputation values for 3xx questions
    breakdown_qs_3xx = config["breakdowns"]["3xx"]

    # TODO: Fix datatypes and remove this temp-fix
    result_211_df.loc[:, breakdown_qs_3xx] = result_211_df.loc[
        :, breakdown_qs_3xx
    ].fillna(0)
    result_211_df.loc[:, breakdown_qs_3xx] = result_211_df.loc[
        :, breakdown_qs_3xx
    ].astype(int)

    # Groupby imp_class again
    trim_grouped = result_211_df.groupby("imp_class")

    expanded_result_df = trim_grouped.apply(
        evaluate_imputed_ixx, "305", break_down_cols=breakdown_qs_3xx
    )

    return expanded_result_df
