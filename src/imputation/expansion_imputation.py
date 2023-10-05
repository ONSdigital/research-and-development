"""_summary_
"""

from src.imputation.tmi_imputation import filter_by_column_content


def run_expansion(df):
    # Step 4: Expansion imputation for breakdown questions

    # Identify clean responders (by status) again

    clear_statuses = ["210", "211"]
    clear_resp_df = filter_by_column_content(df, "statusencoded", clear_statuses)

    # Group by imp_class
    clear_imp_class_grps = clear_resp_df.groupby("imp_class")
    class_keys = list(clear_imp_class_grps.groups.keys())

    for k in class_keys:
        # Get subgroup dataframe
        subgrp_df = clear_imp_class_grps.get_group(k)

        # TODO: remove this temporary fix to cast Nans to False
        subgrp_df["211_trim"] = subgrp_df["211_trim"].fillna(False)

        # Filter to exclude the same rows trimmed for 211_trim == True
        trimmed_df = subgrp_df.loc[subgrp_df["211_trim"]]

        print(trimmed_df)
