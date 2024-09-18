"""
Regression test to compare two versions of outputs
Reads two csv files, old and new
Selects the columns of interest
Joins old and new on key columns, outer
Checks which records are in old only (left), new only (right) or both
Compares if the old and new values are the same within tolerance
Saves the ouotputs
"""

#%% Configuration settings
import pandas as pd

# Input folder and file names
root_path = "R:/BERD Results System Development 2023/DAP_emulation/2023_surveys/BERD/06_imputation/imputation_qa/"
in_file_old = "2023_full_responses_imputed_24-09-10_v764.csv"
in_file_new = "tmp_qa_output2.csv"

# Output folder and file
out_fol = root_path
out_file = "imputation_breakdown_check.csv"

# Columns to select
key_cols = ["reference", "instance"]
value_col = "211"
other_cols = [
    "200",
    "201",
    "formtype",
    "imp_class",
    "imp_marker"
    "status",
]
tolerance = 0.001
#%% Read files
cols_read = key_cols + [value_col] + other_cols
df_old = pd.read_csv(root_path + in_file_old)
df_new = pd.read_csv(root_path + in_file_new)

#%% join old and new
df_merge = df_old.merge(df_new, on=key_cols, how="inner", suffixes=("_old", "_new"))

#%%
df_merge.to_csv(root_path + out_file, index=False)


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
