"""_summary_
"""

from src.imputation.tmi_imputation import filter_by_column_content


def evaluate_imputed_2xx(group, cols_to_sum):
    """Evaluate the imputed 2xx as the sum of all 2xx over the sum of all 211
    multiplied by the imputed 211."""
    sum_211 = group.loc["211"]
    sum_2xx = group.loc[:, cols_to_sum].sum(axis=1)

    return (sum_2xx / sum_211) * group["211_imputed"]


def run_expansion(df, config):
    # Step 4: Expansion imputation for breakdown questions

    # Identify clean responders (by status) again

    clear_statuses = ["210", "211"]
    clear_resp_df = filter_by_column_content(df, "statusencoded", clear_statuses)

    # TODO: remove this temporary fix to cast Nans to False
    clear_resp_df = clear_resp_df["211_trim"].fillna(False)

    # Filter to exclude the same rows trimmed for 211_trim == True
    trimmed_df = clear_resp_df.loc[df["211_trim"]]

    # Get all the 2xx cols, minus 211, imputed vals and trim marker
    breakdown_qs = config["2xx_breakdowns"]

    # Evaluate imputed 2xx
    trimmed_df["imputed_2xx"] = (
        trimmed_df.groupby("imp_class")
        .apply(evaluate_imputed_2xx, args=breakdown_qs)
        .reset_index(drop=True)
    )

    print(trimmed_df)
