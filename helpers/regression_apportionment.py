"""
Regression test to compare two versions of outputs
Reads two csv files, old and new
Selects the columns of interest
Joins old and new on key columns, outer
Checks which records are in old only (left), new only (right) or both
Compares if the old and new values are the same within tolerance
Saves the ouotputs
"""
import sys
sys.path.append('D:/programming_projects/research-and-development')
#%%
import pandas as pd
from src.utils.local_file_mods import read_local_csv as read_csv
from src.utils.local_file_mods import write_local_csv as write_csv

#%% Configuration settings

# Input folder and file names
root_folder = "R:/BERD Results System Development 2023/DAP_emulation/"
in_fol = "apportionment/apportionment_qa/"
in_file_old = "estimated_df_apportioned_2024-02-05_v15.csv"
in_file_new = "estimated_df_apportioned_2024-02-05_v472_TEST.csv"

# Output folder and file
out_fol = "D:/coding_projects/randd_regression/"
out_file = "apportion_merged.csv"

# Columns to select
key_cols = ["reference", "200",  "201", "formtype", "pg_numeric", "601"]
value_col = "211"
other_cols = ["instance", "status", "imp_marker",  "602", "referencepostcode", "postcodes_harmonised"]
tolerance = 0.001
#%% Read files
cols_read = key_cols + [value_col] + other_cols
df_old = read_csv(root_folder + in_fol + in_file_old)[cols_read]
df_new = read_csv(root_folder + in_fol + in_file_new)[cols_read]

#%% Filter good statuses only
imp_markers_to_keep = ["TMI", "CF", "MoR", "constructed"]
df_old_good = df_old[df_old["imp_marker"].isin(imp_markers_to_keep)]
df_new_good = df_new[df_new["imp_marker"].isin(imp_markers_to_keep)]

#%% sizes
print(f"Old size: {df_old_good.shape}")
print(f"New size: {df_new_good.shape}")

#%% Join
df_merge = df_old_good.merge(
    df_new_good,
    on=key_cols,
    how="outer",
    suffixes=("_old", "_new"),
    indicator=True
)
#%% Compare the values
df_merge["value_different"] = (
    (df_merge[value_col + "_old"] - df_merge[value_col + "_new"])**2 > tolerance**2
)

# %% Save output
write_csv(out_fol + out_file, df_merge)

# %%
