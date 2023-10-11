"""_summary_
"""

import logging
import numpy as np

from src.imputation.tmi_imputation import filter_by_column_content
from src.utils.wrappers import df_change_func_wrap

ExpansionLogger = logging.getLogger(__name__)


def evaluate_imputed_ixx(group, master_col, break_down_cols):
    """Evaluate the imputed 2xx or 3xx as the sum of all 2xx or 3xx
    over the sum of all 211 multiplied by the imputed 211."""

    # Make cols into str just in case coming through as ints
    bd_cols = [str(col) for col in break_down_cols]

    # Make imputed column names
    imp_cols = [f"{col}_imputed" for col in bd_cols]

    sum_master_q = np.sum(group[master_col])  # scalar

    for imp_col, bd_col in zip(imp_cols, bd_cols):
        sum_breakdown_q = np.sum(group[bd_col])
        group[imp_col] = (sum_breakdown_q / sum_master_q) * group[
            f"{master_col}_imputed"
        ]

    return group


@df_change_func_wrap
def only_trimmed_records(df, trim_bool_col):
    """Trims the dataframe to only include records that have False in the
    trim_bool_col column"""

    return df.loc[~df[trim_bool_col]]


def run_expansion(df, config):
    # Step 4: Expansion imputation for breakdown questions

    # Identify clean responders (by status) again

    clear_statuses = ["210", "211"]
    clear_resp_df = filter_by_column_content(df, "statusencoded", clear_statuses)

    # TODO: remove this temporary fix to cast Nans to False
    clear_resp_df["211_trim"].fillna(False, inplace=True)

    # Filter to exclude the same rows trimmed for 211_trim == False
    trimmed_df = only_trimmed_records(clear_resp_df, "211_trim")
    ExpansionLogger.debug(f"There are {trimmed_df.shape[0]} rows in the trimmed_df")

    # Trimmed groups
    trim_grouped = trimmed_df.groupby("imp_class")

    # result_df = (trim_grouped.transform(
    # lambda group: evaluate_imputed_2xx_2(group, breakdown_qs)))

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
