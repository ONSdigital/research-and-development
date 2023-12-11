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
    "pg_num": "pg_numeric",
}
# To do: a dictionary seems an overkill. Better idea: juts have variables:
ref = "reference"
ins = "instance"
period = "period"
# form = "formtype"
postcode = "postcodes_harmonised"
# percent = "602"
product = "201"
pg_num = "pg_numeric"
civdef = "200"

# instead of col_name_reference["form"] just write form and so on
# can do a batch find and replace later


# Constants
short_code = "0006"
long_code = "0001"

# Value columns that we want to apportion
want_cals = [str(x) for x in range(202, 509)]


#%%Cleaning the short forms
def apply_short_percent(df: pd.DataFrame, short_percent = 100.0) -> pd.DataFrame:
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

#%% Counting unique non-blank codes
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
    return df

# def count_unique_codes_in_col(df: pd.DataFrame, col_to_count: str = "postcode") -> pd.DataFrame:
#     """
#     Calculates the number of unique non-empty codes.

#     Parameters:
#     df (pd.DataFrame): The input DataFrame.
#     code (str): The column name to count unique codes from.

#     Returns:
#     pd.DataFrame: The DataFrame with an additional column showing the count of unique codes.
#     """
#     # Select the necessary columns and rows
#     dfa = df.loc[df[col_to_count].str.strip().str.len() > 0, [col_name_reference["ref"], col_name_reference["period"], col_name_reference[col_to_count]]]

#     # Count the number of unique non-empty codes
#     dfb = dfa.groupby([col_name_reference["ref"], col_name_reference["period"]])[col_name_reference[col_to_count]].nunique().reset_index()

#     # Rename the count column
#     dfb.rename(columns={col_name_reference[col_to_count]: col_name_reference[col_to_count] + "_count"}, inplace=True)

#     # Merge the count column back into the original DataFrame
#     df = df.merge(dfb, on=[col_name_reference["ref"], col_name_reference["period"]], how="left")

#     return df

# 
def value_to_sites(df : pd.DataFrame, vc: str) -> pd.DataFrame:
    """
    Distributes a column's total value across multiple sites proportionally based on site weights.

    This function takes a DataFrame and a column name as input. It first replaces any NaN values in the 
    specified column with 0. Then, it computes the total value of this column for each group of 'ref' and 
    'period'. This total is then distributed across the sites according to their 'site_weight'. Finally, it 
    replaces any 0 values with NaN.

    Args:
        dfc (pd.DataFrame): The input DataFrame, which must contain columns for 'ref', 'period', the 
                            specified value column, and 'site_weight'.
        vc (str): The name of the column whose values are to be distributed.

    Returns:
        pd.DataFrame: The DataFrame with an additional column showing the distributed values.
    """
    # Create a new column name by appending "_site" to the input column name
    vcs = vc + "_site"
    
    # Replace any NaN values in the input column with 0
    df[vcs] = df[vc].fillna(0)
    
    # Group the DataFrame by 'ref' and 'period', compute the sum of the new column in each group,
    # and store these sums in the new column
    df[vcs] = df.groupby([col_name_reference["ref"], col_name_reference["period"]])[vcs].transform("sum")
    
    # Multiply the new column by the 'site_weight' column to distribute the values across the sites
    df[vcs] = df[vcs] * df["site_weight"]
    
    # Replace any 0 values in the new column with NaN
    df[vcs].replace(0, np.nan, inplace=True)
    
    # Return the modified DataFrame
    return df

# Calculate weights
def weights(df):
    dfc = df.copy()
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
    dfc["site_weight"] = dfc["s_percent"] / dfc["s_percent_total"]

    # Remove unnecessary
    # dfc.drop(columns=['s_percent', 's_percent_total'], inplace=True)    

    return dfc

def copy_vals_across_instances(df, cols):
    """
    Copies the max value of specified columns across instances grouped by 'ref' and 'period'.

    This function takes a DataFrame and a list of column names as input. For each column, it fills any 
    missing values with an empty string and converts the column to str type. 
    Then, it computes the maximum value of the column for each group of 'ref' and 'period'. 
    
    The maximum values are stored in a new column with the suffix "_site" added to the original column name.

    Args:
        df (pd.DataFrame): The input DataFrame, which must contain columns for 'ref', 'period', and the 
                           specified columns.
        cols (list): A list of column names to process.

    Returns:
        pd.DataFrame: The DataFrame with additional columns showing the maximum values.
    """
    # Loop over each column in the input list
    for col in cols:
        # Get the actual column name from the reference dictionary
        col_number = col_name_reference[col]
        
        # Fill any missing values in the column with an empty string
        df[col_number].fillna("", inplace=True)
        
        # Convert the column to string type
        df[col_number] = df[col_number].astype("str")
        
        # Group the DataFrame by 'ref' and 'period', compute the max value in each group for the current column,
        # and store these max values in a new column with "_site" appended to the original column name
        df[col_number + "_site"] = df.groupby([col_name_reference["ref"],
                                            col_name_reference["period"]])[col_number].transform("max")
                                
    return df


mypath = os.path.join(mydir, in_file)
df = pd.read_pickle(mypath)
print(f"Input df is read. Dataframe shape:\n{df.shape}")

#%% Cleane "NONE" postcodes
df[postcode][df[postcode] == "NONE    "] = ""

#%% Calculate which columns are present and  numeric
df_cols = list(df.columns)
exist_cols = [x for x in want_cals if x in df_cols]
value_cols = [x for x in exist_cols if is_numeric_dtype(df[x])]


#%% Calculate the number of uniqie non-blank codes
for code in ["postcode", "product", "civdef"]:
    df = count_unique_codes_in_col(df, code)

#%% Selecting cases with one product, many sites
dfm = df.copy()

# Dataframe with many sites
dfm = dfm[dfm[col_name_reference["postcode"] + "_count"] > 1]
#dfc = dfm[dfm[col_name_reference["product"] + "_count"] == 1]
#dfd = dfm[dfm[col_name_reference["product"] + "_count"] >= 2]

#%% Calculate the number of unique postcode, civdef combinations
def count_unique_keys_in_group(
    df: pd.DataFrame,
    group_cols: list = [ref, period],
    key_cols: list = [product, civdef]) ->pd.DataFrame:
    """ Calculates the number of unique key combinations per group"""
    dfa = df.copy()

    # Select columns that we need
    dfa = dfa[group_cols + key_cols]

    # Remove empty keys
    for key in key_cols:
        dfa = dfa[dfa[key].str.len() > 0]
    
    # Remove duplicate keys
    dfa.drop_duplicates(inplace=True)

    # Calculate the number of rows in each group and repeat it in every row
    dfa["group_size"] = dfa.groupby(group_cols)[ref].transform("count").fillna(0)

    # Select the group colums and group size
    dfa = dfa[group_cols + key_cols + ["group_size"]]

    # Merge the group size back to the original df
    df = df.merge(dfa, on=(group_cols + key_cols), how="left")

    return df
#%% calculate number of product, civdef combinations in multiple sites
dfm = count_unique_keys_in_group(dfm)
#%% Save many-to-many cases
cond_mm = (
    (dfm["group_size"] >= 2) &
    (dfm[ins] >= 1) &
    (dfm[postcode].str.len() > 0)
)

# Dataframe witm many products - many sites
dfmm = dfm[cond_mm]

# Dataframe with one postcode - many sites
df1m = dfm[~cond_mm]

#%% Dataframe with codes and numerical values
group_cols = [ref, period]
code_cols = [product, civdef, pg_num]
value_cols
df_codes = dfmm.copy()[group_cols + code_cols + value_cols]

#%% Df with references, sites and everyting else
site_cols = [x for x in df_cols if x not in (code_cols + value_cols)]
df_sites = dfmm.copy()[site_cols]

#%% Clean codes
# Remove blank produts
df_codes = df_codes[df_codes[product].str.len() > 0]

#%% De-duplicate by summation
value_dict = {value_col: 'sum' for value_col in value_cols}
df_codes = df_codes.groupby(group_cols + code_cols).agg(value_dict).reset_index()

#%% Site cleaning

# check for postcode duplicates
df_sites["site_count"] = df_sites.groupby(group_cols + [postcode])[postcode].transform("count")
df_duplicate_sites = df_sites[df_sites["site_count"] > 1]
num_duplicate_sites = df_duplicate_sites.shape[0]
if num_duplicate_sites:
    print(f"There are {num_duplicate_sites} duplicate sites")

#%% Apply weights
df_sites = weights(df_sites)

#%%  Merge codes to sites to create a Cartesian product
df_cart = df_sites.merge(df_codes, on=group_cols, how="inner")

#%% Apply weights
for value_col in value_cols:
    df_cart[value_col] = df_cart[value_col] * df_cart["site_weight"]

#%% Restore the original column order
df_cart = df_cart[df_cols]

#%%Append the columns back to the original df
df_out = df1m.append(df_cart, ignore_index=True)

#%% Order by period, ref, instance, ASC
df_out.sort_values(by=[period, ref, ins], ascending=True, inplace=True)
#%% Output
out_file = "df_out.csv"
mypath = os.path.join(mydir, out_file)
df_out.to_csv(mypath, index=None)

print(f"Output is saved")

#%% old code below
# #%% Calculate weights
# dfc = weights(dfc)

# #%% Applying weights
# # Calculate which value columns are in the data and are numeric
# cols_to_apportion = [str(x) for x in range(202, 509)] 
# cols_to_apportion = [col for col in cols_to_apportion if col in dfc.columns]
# cols_to_apportion = [col for col in cols_to_apportion if is_numeric_dtype(dfc[col])]

# # Calculates the apportioned value for all value columns
# for val_col in cols_to_apportion:
#     dfc = value_to_sites(dfc, val_col)

# # Repeat the product group and C or D marker across multiple sites
# key_cols = ["product", "civdef"]
# dfc = copy_vals_across_instances(dfc, key_cols)

# # Chooses the columns to merge back to the original data
# indexcols = [col_name_reference["ref"], col_name_reference["period"], col_name_reference["ins"]]
# svaluecols = [x + "_site" for x in value_cols]
# scodecols = [col_name_reference[col] + "_site" for col in key_cols]
# usecols = indexcols + svaluecols + scodecols
# dfc = dfc[usecols]

# # Merges the apportioned values and repeated code back to the main dataframe
# df_out = df.merge(dfc, on=indexcols, how="left")

# # Replace the values when the apportioned value is not null
# key_names = [col_name_reference[x] for x in col_name_reference if x in key_cols]
# for val_col in value_cols + key_names:
#     _ = df_out.loc[~df_out[val_col + "_site"].isnull(), val_col] = df_out[val_col + "_site"]

# # Removes the columns ending with "_site"
# df_out.drop(columns=(svaluecols + scodecols), inplace=True)

# # Save the output
# mypath = os.path.join(mydir, out_file)
# df_out.to_csv(mypath, index=None)
# print(f"Output is saved")


# %%
