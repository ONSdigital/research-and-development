"""Read in a csv file and ouput a test file with data for a unit test."""
import pandas as pd
import os

from src.imputation import imputation_helpers as hlp

# file configuration settings
runid_old = "2024-02-02_vTEST_467"
runid_new = "2024-02-02_v14"
output_version = "1"

root_path = "R:/BERD Results System Development 2023/DAP_emulation/"
folder_path = "apportionment/apportionment_qa/"
file_root = "estimated_df_apportioned_"
output_folder = "D:/coding_projects/randd_test_data/"

output_filename = f"{file_root}_comparison_{output_version}_{runid_old}_vs_{runid_new}"

# other config settings
join_cols = ["reference", "instance"]
value_cols = ["211"]

# read in the csvs
old_file = os.path.join(root_path, folder_path, f"{file_root}{runid_old}.csv")
new_file = os.path.join(root_path, folder_path, f"{file_root}{runid_new}.csv")

old_df = pd.read_csv(old_file, low_memory=False)
# new_df = pd.read_csv(new_file, low_memory=False)

# filter conditions
def get_mask(df:pd.DataFrame, col:str, values:list):
    return df[col].isin(values)

def filter_df(df:pd.DataFrame, col:str, values:list):
    return df.copy().loc[df[col].isin(values)]

print(filter_df(old_df, "formtype", [1]).head())





