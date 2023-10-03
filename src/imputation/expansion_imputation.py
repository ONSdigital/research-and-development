"""_summary_
"""

from tmi_imputation import filter_by_column_content


def run_expansion(df):
    # Step 4: Expansion imputation for breakdown questions

    # Identify clean responders (by status) again

    clear_statuses = ["210", "211"]
    clear_resp_df = filter_by_column_content(df, "statusencoded", clear_statuses)

    # Group by imp_class
    clear_imp_class_df = clear_resp_df.groupby("imp_class")
    class_keys = list(clear_imp_class_df.groups.keys())

    # Filter to exclude the same rows trimmed for 211_trim == True
    trimmed_df = clear_imp_class_df.loc[clear_imp_class_df["211_trim"]]

    print(class_keys)
    print(trimmed_df.head())
