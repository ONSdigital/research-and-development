"""
Regression test to compare two versions of outputs
Reads two csv files, old and new
Selects the columns of interest
Joins old and new on key columns, outer
Checks which records are in old only (left), new only (right) or both
Compares if the old and new values are the same within tolerance
Saves the ouotputs
"""
# %%
import pandas as pd

# import sys
# sys.path.append("D:/coding_projects/github_repos/research-and-development")

from src.utils.local_file_mods import write_local_csv as write_csv

# %% Configuration settings

# Input folder and file names
root = "R:/BERD Results System Development 2023/DAP_emulation/imputation/back_imp_test/"
inp_22 = "LF_2022_responses_imputed_from_v347.csv"
inp_23 = "LF_responders_2023_from_v381.csv"

# Output folder and file
out_file = "CB_data.csv"

# some global vars
clear_statuses = ["Clear", "Clear - overridden"]

# Columns to select
key_cols = ["reference"]
value_cols = ["211"]
replace_vars = ["instance", "200", "201", "imp_class", "601"]
other_cols = ["formtype", "status"]
tolerance = 0.001
# %% Read files
wanted_cols = key_cols + replace_vars + other_cols
old_wanted_cols = wanted_cols + value_cols + [f"{col}_imputed" for col in value_cols]
new_wanted_cols = wanted_cols + value_cols
df_old = pd.read_csv(root + inp_22, low_memory=False, usecols=old_wanted_cols)
df_new = pd.read_csv(root + inp_23, low_memory=False, usecols=new_wanted_cols)

# %% sizes
print(f"Old size: {df_old.shape}")
print(f"New size: {df_new.shape}")

# %% Join
df_merge = df_old.merge(
    df_new, on=key_cols, how="left", suffixes=("_old", "_new"), indicator=True
)
# ensure the instance columns are still type "int" after merge
df_merge = df_merge.astype({"instance_old": "Int64", "instance_new": "Int64"})


def carry_backwards(df_merge):
    """Create carry backwards values if new values are clean."""
    # the logical condition for new and old values matching in the merge
    match_cond = df_merge["_merge"] == "both"
    # the logical condition for clean 2023 responders
    clear_cond = df_merge["status_new"].isin(clear_statuses)
    cb_cond = match_cond & clear_cond

    # The replace_vars are always carried forward/backwards when doing MoR
    for var in replace_vars:
        df_merge.loc[cb_cond, f"{var}_imputed"] = df_merge.loc[cb_cond, f"{var}_new"]

    # Now  carry backwards the new values
    for var in value_cols:
        df_merge.loc[cb_cond, f"{var}_CB_imputed"] = df_merge.loc[cb_cond, f"{var}_new"]

        # # fill nulls with zeros if col 211 is not null
        # fillna_cond = ~df_merge["211_new"].isnull()
        # df_merge.loc[cb_cond & fillna_cond, f"{var}_CB_imputed"] = df_merge.loc[
        #     cb_cond & fillna_cond, f"{var}_imputed"
        # ].fillna(0)

    df_merge.loc[cb_cond, "imp_marker"] = "CB"
    df_merge = df_merge.drop("_merge", axis=1)

    return df_merge


# Calculate the link factor
def calc_growths(df, vars):
    # select only records that are clear in both 2022 and 2023
    cl_cond = df.status_old.isin(clear_statuses) & df.status_new.isin(clear_statuses)
    # cols = (
    #     ["reference", "imp_class_new"]
    #     + [f"{var}_new" for var in vars]
    #     + [f"{var}_old" for var in vars]
    # )
    df = df.copy().loc[cl_cond]  # [cols]
    for var in value_cols:
        mask = (df[f"{var}_new"] != 0) & (df[f"{var}_old"] != 0)
        df.loc[mask, f"{var}_gr"] = (
            df.loc[mask, f"{var}_old"] / df.loc[mask, f"{var}_new"]
        )
        df[f"{var}_gr"] = df[f"{var}_gr"].fillna(0)

    return df


def group_calc_link(group, target_vars):
    """Apply the MoR method to each group

    Args:
        group (pd.core.groupby.DataFrameGroupBy): Imputation class group
        link_vars ([string]): List of the linked variables.
    """

    for var in target_vars:
        # Create mask to not use 0s in mean calculation
        non_null_mask = pd.notnull(group[f"{var}_gr"])

        num_valid_vars = sum(non_null_mask)
        # If the group is a valid size, and there are non-null, non-zero values for this
        # 'var', then calculate the mean
        if num_valid_vars > 0:
            group[f"{var}_link"] = group.loc[non_null_mask, f"{var}_gr"].mean()
        # Otherwise the link is set to 1
        else:
            group[f"{var}_link"] = 1.0
    return group


def apply_links(cb_df, links_df, target_vars):
    """Apply the links to the carried backwards values.

    Args:
        cb_df (pd.DataFrame): DataFrame of carried forwards values.
        links_df (pd.DataFrame): DataFrame containing calculated links.
        target_vars ([string]): List of target variables.
    """
    # Reduce the mor_df so we only have the variables we need and one row
    # per imputation class
    links_df = (
        links_df[["imp_class_new"] + [f"{var}_link" for var in target_vars]]
        .groupby("imp_class_new")
        .first()
    )

    cb_df = pd.merge(cb_df, links_df, on="imp_class_new", how="left", indicator=True)

    # Mask for values that are CF and also have a MoR link
    matched_mask = (cb_df["_merge"] == "both") & (cb_df["imp_marker"] == "CB")

    # Apply BI for the target variables
    for var in target_vars:
        # Only apply MoR where the link is non null/0
        no_zero_mask = pd.notnull(cb_df[f"{var}_link"]) & (cb_df[f"{var}_link"] != 0)
        full_mask = matched_mask & no_zero_mask
        # Apply the links to the previous values
        cb_df.loc[full_mask, f"{var}_imputed"] = (
            cb_df.loc[full_mask, f"{var}_imputed"] * cb_df.loc[full_mask, f"{var}_link"]
        )
        cb_df.loc[matched_mask, "imp_marker"] = "BI"
        # Drop _merge column
    cb_df = cb_df.drop("_merge", axis=1)
    return cb_df


cb_df = carry_backwards(df_merge)

gr_df = calc_growths(cb_df, value_cols)
group = gr_df.groupby("imp_class_old")
links_df = group.apply(group_calc_link, value_cols)

# apply the links for the final dataframe
imputed_df = apply_links(cb_df, links_df, value_cols)


# %% Save output
write_csv(root + out_file, imputed_df)

# %%
