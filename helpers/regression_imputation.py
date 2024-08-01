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
import pandas as pd

sys.path.append("D:/coding_projects/github_repos/research-and-development")
#%%
import pandas as pd
from src.utils.local_file_mods import rd_read_csv as read_csv
from src.utils.local_file_mods import rd_write_csv as write_csv

#%% Configuration settings

# Input folder and file names
root_folder = "R:/BERD Results System Development 2023/DAP_emulation/2023_surveys/BERD/"
in_fol = "06_imputation/imputation_qa/"
file_stub = "2023_full_responses_imputed"
date_old = "24-08-01"
date_new = "24-08-01"
version_old = "v32"
version_new = "v674"
in_file_old = f"{file_stub}_{date_old}_{version_old}.csv"
in_file_new = f"{file_stub}_{date_new}_{version_new}.csv"

# Output folder and file
out_fol = "R:/BERD Results System Development 2023/DAP_emulation/analysis/"
out_file = f"imputation_merged_{version_old}_{version_new}.csv"

# Columns to select
key_cols = ["reference", "200", "201", "formtype", "cellnumber"]
value_col = "211_imputed"
other_cols = [
    "instance",
    "status",
    "imp_marker",
    "imp_class",
]
tolerance = 0.001
#%% Read files
cols_read = key_cols + [value_col] + other_cols
df_old = pd.read_csv(root_folder + in_fol + in_file_old, usecols = cols_read, low_memory=False)
df_new = pd.read_csv(root_folder + in_fol + in_file_new, usecols = cols_read, low_memory=False)

#%% Filter good statuses only
imp_markers_to_keep = ["TMI", "CF", "MoR", "constructed"]
df_old_good = df_old[df_old["imp_marker"].isin(imp_markers_to_keep)]
df_new_good = df_new[df_new["imp_marker"].isin(imp_markers_to_keep)]

#%% sizes
print(f"Old size: {df_old_good.shape}")
print(f"New size: {df_new_good.shape}")

#%% Join
df_merge = df_old_good.merge(
    df_new_good, on=key_cols, how="outer", suffixes=("_old", "_new"), indicator=True
)
#%% Compare the values
df_merge["value_different"] = (
    df_merge[value_col + "_old"] - df_merge[value_col + "_new"]
) ** 2 > tolerance**2

# %% Save output
write_csv(out_fol + out_file, df_merge)

# %%
