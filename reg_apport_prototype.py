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
cols = {
    "ref": "reference",
    "ins": "instance",
    "period": "period",
    "form": "formtype",
    "postcode": "postcodes_harmonised",
    "percent": "602",
    "product": "201",
    "civdef": "200",
}

want_cals = [str(x) for x in range(202, 509)] 

short_code = "0006"
long_code = "0001"
short_percent = 100.0


# %%  Functions

# Cleaning the short forms
def run_short(df: pd.DataFrame) -> pd.DataFrame:
    df_out = df.copy()
    cond = (df_out[cols["form"]] == short_code)
    df_out[cols["percent"]].mask(cond, other=short_percent, inplace=True)
    return df_out

# Counting unique non-blank codes
def count_codes(df: pd.DataFrame, code: str = "postcode") -> pd.DataFrame:
    # Calculates the number of unique non-empty codes

    dfa = df.copy()
    # Select columns that we need
    cols_need = [cols["ref"], cols["period"], cols[code]]
    dfa = dfa[cols_need]
    dfa = dfa[dfa[cols[code]].str.len() > 0]
    dfa.drop_duplicates(inplace=True)
    dfb = dfa.groupby([cols["ref"], cols["period"]]).agg("count").reset_index()
    dfb.rename({cols[code]: cols[code] + "_count"}, axis='columns', inplace=True)
    df = df.merge(
        dfb,
        on = [cols["ref"], cols["period"]],
        how="left")
    return df

# Computes the total numerical value across multiple sites and spreads it using 
# the weights
def value_to_sites(dfc : pd.DataFrame, vc: str) -> pd.DataFrame:
    vcs = vc + "_s"
    dfc[vcs] = dfc[vc].fillna(0)
    dfc[vcs] = dfc.groupby([cols["ref"], cols["period"]])[vcs].transform("sum")
    dfc[vcs] = dfc[vcs] * dfc["s_weight"]
    dfc[vcs].replace(0, np.nan, inplace=True)
    return dfc

# Calculate weights
def weights(dfc):
    dfc["s_percent"] = dfc[cols["percent"]]
    dfc["s_percent"].fillna(0, inplace=True)

    # Set the weight for instance 0 to be 0
    dfc["s_percent"] = dfc["s_percent"] * dfc[cols["ins"]].astype("bool")

    #%% Calculate the total percent for each reference and period
    dfc["s_percent_total"] = (
        dfc.groupby([cols["ref"], cols["period"]])["s_percent"].transform("sum"))

    # Filter out the rows where total percent is zero
    dfc= dfc[dfc["s_percent_total"] != 0]

    # Compute weights
    dfc["s_weight"] = dfc["s_percent"] / dfc["s_percent_total"]

    return dfc

#%% Load input data
mypath = os.path.join(mydir, in_file)
df = pd.read_pickle(mypath)
print(f"Input df is read. Columns are:\n{df.dtypes}")

#%% Main function
# Assign 100 to the "percent" column of short forms 
df_out = run_short(df)

#%% Apportionment of long forms 
# Extract the long forms
df = df[df[cols["form"]] == long_code]

# Count distinct non-empty codes
for code in ["postcode", "product", "civdef"]:
    df = count_codes(df, code)

# Selecting cases with one product, many sites
dfc = df.copy()
dfc = dfc[
    (dfc[cols["postcode"] + "_count"] > 1) &
    (dfc[cols["product"] + "_count"] == 1)
]

# Calculate weights
dfc = weights(dfc)

# Applying weights
# Calculate which value columns are in the data and are numeric
exist_cols = [x for x in want_cals if x in dfc.columns]
value_cols = [x for x in exist_cols if is_numeric_dtype(dfc[x])]

# Calculates the apportioned value for all value columns
for vc in value_cols:
    dfc = value_to_sites(dfc, vc)

# Repeat the product group and C or D marker across multiple sites
key_cols = ["product", "civdef"]
for col in key_cols:
    c = cols[col]
    dfc[c].fillna("", inplace=True)
    dfc[c].astype("str")
    dfc[c + "_s"] = (
        dfc
        .groupby([cols["ref"], cols["period"]])[c]
        .transform("max"))

# Chooses the columns to merge back to the original data
indexcols = [cols["ref"], cols["period"], cols["ins"]]
svaluecols = [x + "_s" for x in value_cols]
scodecols = [cols[col] + "_s" for col in key_cols]
usecols = indexcols + svaluecols + scodecols
dfc = dfc[usecols]

# Merges the apportioned values and repeated code back to the main dataframe
df_out = df_out.merge(dfc, on=indexcols, how="left")

# Replace the values and remove the columns _s
key_names = [cols[x] for x in cols if x in key_cols]
for vc in value_cols + key_names:
    _ = df_out.loc[~df_out[vc + "_s"].isnull(), vc] = df_out[vc + "_s"]

# Removes the columns with underscore _s
df_out.drop(columns=(svaluecols + scodecols), inplace=True)

# Save the output
mypath = os.path.join(mydir, out_file)
df_out.to_csv(mypath, index=None)
print(f"Output is saved")


# %%
