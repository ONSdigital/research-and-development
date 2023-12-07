#%% -*- coding: utf-8 -*-
"""
Created on Wed Nov 22 11:46:52 2023
@author: zoring

"""
import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype

import os
#%% Loading ddata
mydir = r"D:\data\res_dev\outputs\reg_apport"
in_file = "outputs_df_before.pkl"
out_file = "outputs_df_corrected.csv"

#%% Parameters
col_name_reference = {
    "ref": "reference",
    "ins": "instance",
    "period": "period",
    "form": "formtype",
    "postcode": "postcodes_harmonised",
    "percent": "602",
    "product": "201",
    "civdef": "200",
}

# Constants
short_code = "0006"
long_code = "0001"




# Cleaning the short forms
def apply_short_percent(df: pd.DataFrame, short_percent = 100.0is_n) -> pd.DataFrame:
    """
    Apply a specific percentage value to rows in a DataFrame where a certain condition is met.

    This function takes a DataFrame as input, makes a copy of it, and then modifies the copy. 
    The modification involves replacing values in the 'percent' column with a predefined value 
    (`short_percent`) for rows where the 'form' column equals a predefined code (`short_code`).

    Parameters:
    df (pd.DataFrame): Thef input DataFrame.

    Returns:
    pd.DataFrame: The modified DataFrame.
    """
    df_out = df.copy()
    cond = (df_out[col_name_reference["form"]] == short_code)
    df_out[col_name_reference["percent"]].mask(cond, other=short_percent, inplace=True)
    return df_out

# Counting unique non-blank codes
def count_unique_codes_in_col(df: pd.DataFrame, code: str = "postcode") -> pd.DataFrame:
    # Calculates the number of unique non-empty codes

    dfa = df.copy()
    # Select columns that we need
    cols_need = [col_name_reference["ref"], col_name_reference["period"], col_name_reference[code]]
    dfa = dfa[cols_need]
    dfa = dfa[dfa[col_name_reference[code]].str.len() > 0]
    dfa.drop_duplicates(inplace=True)
    dfb = dfa.groupby([col_name_reference["ref"], col_name_reference["period"]]).agg("count").reset_index()
    dfb.rename({col_name_reference[code]: col_name_reference[code] + "_count"}, axis='columns', inplace=True)
    df = df.merge(
        dfb,
        on = [col_name_reference["ref"], col_name_reference["period"]],
        how="left")
    return dfcount_codes

def count_unique_codes_in_col(df: pd.DataFrame, col_to_count: str = "postcode") -> pd.DataFrame:
    """
    Calculates the number of unique non-empty codes.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    code (str): The column name to count unique codes from.

    Returns:
    pd.DataFrame: The DataFrame with an additional column showing the count of unique codes.
    """
    # Select the necessary columns and rows
    dfa = df.loc[df[col_to_count].str.strip().str.len() > 0, [col_name_reference["ref"], col_name_reference["period"], col_name_reference[col_to_count]]]

    # Count the number of unique non-empty codes
    dfb = dfa.groupby([col_name_reference["ref"], col_name_reference["period"]])[col_name_reference[col_to_count]].nunique().reset_index()

    # Rename the count column
    dfb.rename(columns={col_name_reference[col_to_count]: col_name_reference[col_to_count] + "_count"}, inplace=True)

    # Merge the count column back into the original DataFrame
    df = df.merge(dfb, on=[col_name_reference["ref"], col_name_reference["period"]], how="left")

    return df

# 
def value_to_sites(dfc : pd.DataFrame, vc: str) -> pd.DataFrame:
    """
    Distributes a column's total value across multiple sites proportionally based on site weights.

    This function takes a DataFrame and a column name as input. It first replaces any NaN values in the 
    specified column with 0. Then, it computes the total value of this column for each group of 'ref' and 
    'period'. This total is then distributed across the sites according to their 's_weight'. Finally, it 
    replaces any 0 values with NaN.

    Args:
        dfc (pd.DataFrame): The input DataFrame, which must contain columns for 'ref', 'period', the 
                            specified value column, and 's_weight'.
        vc (str): The name of the column whose values are to be distributed.

    Returns:
        pd.DataFrame: The DataFrame with an additional column showing the distributed values.
    """
    vcs = vc + "_sum"
    dfc[vcs] = dfc[vc].fillna(0)
    dfc[vcs] = dfc.groupby([col_name_reference["ref"], col_name_reference["period"]])[vcs].transform("sum")
    dfc[vcs] = dfc[vcs] * dfc["s_weight"]
    dfc[vcs].replace(0, np.nan, inplace=True)
    return dfc

# Calculate weights
def weights(dfc):
    dfc["s_percent"] = dfc[col_name_reference["percent"]]
    dfc["s_percent"].fillna(0, inplace=True)

    # Set the weight for instance 0 to be 0
    dfc["s_percent"] = dfc["s_percent"] * dfc[col_name_reference["ins"]].astype("bool")

    #%% Calculate the total percent for each reference and period
    dfc["s_percent_total"] = (
        dfc.groupby([col_name_reference["ref"], col_name_reference["period"]])["s_percent"].transform("sum"))

    # Filter out the rows where total percent is zero
    dfc= dfc[dfc["s_percent_total"] != 0]

    # Compute weights
    dfc["s_weight"] = dfc["s_percent"] / dfc["s_percent_total"]

    return dfc

# Repeat keys
def copy_vals_across_instances(df, cols):
    """
    Copies the max value of specified columns across instances grouped by 'ref' and 'period'.

    This function takes a DataFrame and a list of column names as input. For each column, it fills any 
    missing values with an empty string and converts the column to str type. 
    Then, it computes the maximum value of the column for each group of 'ref' and 'period'. 
    
    The maximum values are stored in a new column with the suffix "_s" added to the original column name.

    Args:
        df (pd.DataFrame): The input DataFrame, which must contain columns for 'ref', 'period', and the 
                           specified columns.
        cols (list): A list of column names to process.

    Returns:
        pd.DataFrame: The DataFrame with additional columns showing the maximum values.
    """
    for col in cols:
        c = col_name_reference[col]
        df[c].fillna("", inplace=True)
        df[c] = df[c].astype("str")
        df[c + "_s"] = df.groupby([col_name_reference["ref"], 
                                     col_name_reference["period"]])[c].transform("max")
    return df


mypath = os.path.join(mydir, in_file)
df = pd.read_pickle(mypath)
print(f"Input df is read. Columns are:\n{df.dtypes}")


#%% Apportionment of long forms 
# Extract the long forms
df = df[df[col_name_reference["form"]] == long_code]

# Count distinct non-empty codes
for code in ["postcode", "product", "civdef"]:
    df = count_unique_codes_in_col(df, code)

# Selecting cases with one product, many sites
dfm = df.copy()
dfm = dfm[dfm[col_name_reference["postcode"] + "_count"] > 1]
dfc = dfm[dfm[col_name_reference["product"] + "_count"] == 1]
dfd = dfm[dfm[col_name_reference["product"] + "_count"] >= 2]
# Calculate weights
dfc = weights(dfc)

# Applying weights
# Calculate which value columns are in the data and are numeric
cols_to_apportion = [str(x) for x in range(202, 509)] 
cols_to_apportion = [col for col in cols_to_apportion if col in dfc.columns]
cols_to_apportion = [col for col in cols_to_apportion if is_numeric_dtype(dfc[col])]

# Calculates the apportioned value for all value columns
for val_col in cols_to_apportion:
    dfc = value_to_sites(dfc, val_col)

# Repeat the product group and C or D marker across multiple sites
key_cols = ["product", "civdef"]
dfc = copy_vals_across_instances(dfc, key_cols)

# Chooses the columns to merge back to the original data
indexcols = [col_name_reference["ref"], col_name_reference["period"], col_name_reference["ins"]]
svaluecols = [x + "_s" for x in value_cols]
scodecols = [col_name_reference[col] + "_s" for col in key_cols]
usecols = indexcols + svaluecols + scodecols
dfc = dfc[usecols]

# Merges the apportioned values and repeated code back to the main dataframe
df_out = df_out.merge(dfc, on=indexcols, how="left")

# Replace the values when the apportioned value is not null
key_names = [col_name_reference[x] for x in col_name_reference if x in key_cols]
for val_col in value_cols + key_names:
    _ = df_out.loc[~df_out[val_col + "_s"].isnull(), val_col] = df_out[val_col + "_s"]

# Removes the columns ending with "_s"
df_out.drop(columns=(svaluecols + scodecols), inplace=True)

# Save the output
mypath = os.path.join(mydir, out_file)
df_out.to_csv(mypath, index=None)
print(f"Output is saved")


# %%
