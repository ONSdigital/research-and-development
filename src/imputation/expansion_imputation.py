"""_summary_
"""

import logging

from src.imputation.tmi_imputation import filter_by_column_content
from src.utils.wrappers import df_change_func_wrap

ExpansionLogger = logging.getLogger(__name__)


def evaluate_imputed_2xx(group, break_down_q):
    """Evaluate the imputed 2xx as the sum of all 2xx over the sum of all 211
    multiplied by the imputed 211."""
    sum_211 = group["211"].sum()  # scalar
    sum_2xx = group[str(break_down_q)].sum()  # scalar

    imputed = (sum_2xx / sum_211) * group["211_imputed"]

    group[f"imputed_{break_down_q}"] = imputed

    return group


def evaluate_imputed_ixx(group, master_col, break_down_cols):
    """Evaluate the imputed 2xx as the sum of all 2xx over the sum of all 211
    multiplied by the imputed 211."""

    # Make cols into str just in case coming through as ints
    bd_cols = [str(col) for col in break_down_cols]

    # Make imputed column names
    imp_cols = [f"{col}_imputed" for col in bd_cols]

    sum_211 = group[master_col].sum()  # scalar

    for imp_col, bd_col in zip(imp_cols, bd_cols):
        group[imp_col] = (group[bd_col].sum() / sum_211) * group["211_imputed"]

    return group


@df_change_func_wrap
def only_trimmed_records(df, trim_bool_col):
    """Trims the dataframe to only include records that have True in the
    trim_bool_col column"""

    return df.loc[df[trim_bool_col]]


def run_expansion(df, config):
    # Step 4: Expansion imputation for breakdown questions

    # Identify clean responders (by status) again

    clear_statuses = ["210", "211"]
    clear_resp_df = filter_by_column_content(df, "statusencoded", clear_statuses)

    # TODO: remove this temporary fix to cast Nans to False
    clear_resp_df["211_trim"].fillna(False, inplace=True)

    # Filter to exclude the same rows trimmed for 211_trim == True
    trimmed_df = only_trimmed_records(clear_resp_df, "211_trim")

    # Trimmed groups
    trim_grouped = trimmed_df.groupby("imp_class")

    # result_df = (trim_grouped.transform(
    # lambda group: evaluate_imputed_2xx_2(group, breakdown_qs)))

    # Calculate the imputation values for 2xx questions
    breakdown_qs_2xx = config["2xx_breakdowns"]
    result_df = trim_grouped.apply(
        evaluate_imputed_ixx, "211", break_down_cols=breakdown_qs_2xx
    )

    # Calculate the imputation values for 3xx questions
    breakdown_qs_3xx = config["3xx_breakdowns"]
    result_df = trim_grouped.apply(
        evaluate_imputed_ixx, "305", break_down_cols=breakdown_qs_3xx
    )

    print(result_df)
