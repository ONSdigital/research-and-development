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
import sys

sys.path.append("D:/coding_projects/github_repos/research-and-development")  # noqa

from src.utils.local_file_mods import write_local_csv as write_csv  # noqa

# %% Configuration settings

# Input folder and file names
root = "R:/BERD Results System Development 2023/DAP_emulation/imputation/back_imp_test/"
inp_22 = "LF_2022_responses_imputed_from_v347.csv"
inp_23 = "LF_responders_2023_from_v381.csv"

# Output folder and file
out_file = "backward_imp_output_AG.csv"

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
imp_cols = [f"{col}_imputed" for col in value_cols]
old_wanted_cols = wanted_cols + value_cols + imp_cols + ["imp_marker"]
new_wanted_cols = wanted_cols + value_cols
df_old = pd.read_csv(root + inp_22, low_memory=False, usecols=old_wanted_cols)
df_new = pd.read_csv(root + inp_23, low_memory=False, usecols=new_wanted_cols)

# %% sizes
print(f"Old size: {df_old.shape}")
print(f"New size: {df_new.shape}")

# %% Join


def carry_backwards(df_old, df_new_clear: pd.DataFrame) -> pd.DataFrame:
    """Create carry backwards values if new values are clear."""
    # the logical condition for new and old values matching in the merge

    df_merge = df_old.copy().merge(
        df_new_clear, on=key_cols, how="left", suffixes=("_old", "_new"), indicator=True
    )
    # ensure the instance columns are still type "int" after merge
    df_merge = df_merge.astype({"instance_old": "Int64", "instance_new": "Int64"})
    cb_cond = (df_merge["_merge"] == "both") & (df_merge.imp_marker == "TMI")
    # The replace_vars are always carried forward/backwards when doing MoR
    for var in replace_vars:
        df_merge.loc[cb_cond, f"{var}_CB_imputed"] = df_merge.loc[cb_cond, f"{var}_new"]

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
def calc_growths(old_df, new_df, vars: list) -> pd.DataFrame:
    # select only records that are clear in both 2022 and 2023
    old_df = (
        old_df.copy()[["reference", "imp_class"] + vars]
        .groupby(["reference", "imp_class"])
        .sum()
    ).reset_index()

    new_df = (
        new_df.copy()[["reference", "imp_class"] + vars]
        .groupby(["reference", "imp_class"])
        .sum()
    ).reset_index()
    df = pd.merge(
        old_df,
        new_df,
        on=["reference", "imp_class"],
        how="inner",
        suffixes=("_old", "_new"),
        validate="one_to_one",
    )
    # cols = (
    #     ["reference", "imp_class_new"]
    #     + [f"{var}_new" for var in vars]
    #     + [f"{var}_old" for var in vars]
    # )
    # df = df.copy().loc[cl_cond & ins_cond]  # [cols]
    for var in vars:
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
        non_null_mask = group[f"{var}_gr"] > 0.001

        num_valid_vars = sum(non_null_mask)
        group[f"{var}_count"] = num_valid_vars
        # If the group is a valid size, and there are non-null, non-zero values for this
        # 'var', then calculate the mean
        if num_valid_vars > 4:
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

    cb_df = cb_df.rename(columns={"imp_class_CB_imputed": "imp_class"})
    cb_df = cb_df.merge(links_df, on="imp_class", how="left", indicator=True)

    # Mask for values that are CB and also have a MoR link
    matched_mask = (cb_df["_merge"] == "both") & (cb_df["imp_marker"] == "CB")

    # Apply BI for the target variables
    for var in target_vars:
        # Only apply MoR where the link is non null/0
        no_zero_mask = pd.notnull(cb_df[f"{var}_link"]) & (cb_df[f"{var}_link"] != 0)
        full_mask = matched_mask & no_zero_mask
        # Apply the links to the previous values
        cb_df.loc[full_mask, f"{var}_BI_imputed"] = (
            cb_df.loc[full_mask, f"{var}_CB_imputed"]
            * cb_df.loc[full_mask, f"{var}_link"]
        )
        cb_df.loc[matched_mask, "imp_marker"] = "BI"
        # Drop _merge column
    cb_df = cb_df.drop("_merge", axis=1)
    return cb_df


new_cl_cond = df_new.status.isin(clear_statuses) & (df_new.instance != 0)
old_cl_cond = df_old.status.isin(clear_statuses) & (df_old.instance != 0)

df_new_clear = df_new.copy().loc[new_cl_cond]
df_old_clear = df_old.copy().loc[old_cl_cond]

# carry out the Carry Backwards imputation
cb_df = carry_backwards(df_old, df_new_clear)

# calculate the growths from clear statuses in old and new data
gr_df = calc_growths(df_old_clear, df_new_clear, value_cols)
write_csv(root + "growths.csv", gr_df)

# calculate a dataframe with links and counts for each imputation class
group = gr_df.groupby("imp_class")
links_df = group.apply(group_calc_link, value_cols)

cols = ["imp_class"]
for var in value_cols:
    cols.append(f"{var}_link")
    cols.append(f"{var}_count")
links_df = links_df[cols].groupby("imp_class").first().reset_index()
write_csv(root + "imputation_links_AG.csv", links_df)


# apply the links for the final dataframe
imputed_df = apply_links(cb_df, links_df, value_cols)


# %% Save output
write_csv(root + out_file, imputed_df)

# %%
