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
ref_col = "reference"
ins_col = "instance"
period_col = "period"
form_col = "formtype"
pc_col = "postcodes_harmonised"
percent_col = "602"

want_cals = [str(x) for x in range(202, 251)] + [str(x) for x in range(300, 509)] 

#value_cols = ["211", "407", "408", "409"]

short_code = "0006"
long_code = "0001"
short_percent = 100.0


# %%  Functions
def run_short(df):
    cond = (df[cols["form"]] == short_code)
    df[cols["percent"]].mask(cond, other=short_percent, inplace=True)
    return df


#%%
mypath = os.path.join(mydir, in_file)
df = pd.read_pickle(mypath)
print(f"Input df is read. Columns are:\n{df.dtypes}")

#%% Cases
cases = {}

#%% Case A Assign 100 to short forms
cases.update({"short form": run_short})

#%%
for key in cases:
    df_out = cases[key](df)

#%% Extract the long forms

cond = df[cols["form"]] == long_code

df = df[cond]

#%% Extract the forms that have multiple postcodes and single PG
def count_codes(df: pd.DataFrame, code: str = "postcode"):
    # Calculates the number of unique non-empty codes

    # Calculate the number of unique non-zero postcodes 
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
#%%
df = count_codes(df, "postcode")
df = count_codes(df, "product")
df = count_codes(df, "civdef")

#%% Compute weights
dfc = df.copy()
dfc = dfc[
    (dfc[cols["postcode"] + "_count"] > 1) &
    (dfc[cols["product"] + "_count"] == 1)
]

#% Create a separate column for site weights
dfc["s_percent"] = dfc[cols["percent"]]
dfc["s_percent"].fillna(0, inplace=True)
#%% Set the weight for instance 0 to be 0
dfc["s_percent"] = dfc["s_percent"] * dfc[cols["ins"]].astype("bool")

#%% Calculate the total per ref per period
dfc["s_percent_total"] = (
    dfc.groupby([cols["ref"], cols["period"]])["s_percent"].transform("sum"))

#%% Filter out the rows where total percent is zero

dfc= dfc[dfc["s_percent_total"] != 0]

#%% Compute weights
dfc["s_weight"] = dfc["s_percent"] / dfc["s_percent_total"]

#%% Apply weights - create new columns
def value_to_sites(dfc : pd.DataFrame, vc: str) -> pd.DataFrame:
    vcs = vc + "_s"
    dfc[vcs] = dfc[vc].fillna(0)
    dfc[vcs] = dfc.groupby([cols["ref"], cols["period"]])[vcs].transform("sum")
    dfc[vcs] = dfc[vcs] * dfc["s_weight"]
    dfc[vcs].replace(0, np.nan, inplace=True)
    return dfc

#%% Calculate whoch columns are numeric
exist_cols = [x for x in want_cals if x in dfc.columns]
value_cols = [x for x in exist_cols if is_numeric_dtype(dfc[x])]


#%% Apportion columns to sites
for vc in value_cols:
    dfc = value_to_sites(dfc, vc)

#%% Repeat the product group
dfc[cols["product"]].fillna("", inplace=True)
dfc[cols["product"]].astype("str")
dfc[cols["product"] + "_s"] = (
    dfc
    .groupby([cols["ref"], cols["period"]])[cols["product"]]
    .transform("max"))

#%% Replace initial values with apportioned values
indexcols = [cols["ref"], cols["period"], cols["ins"]]
svaluecols = [x + "_s" for x in value_cols]
usecols = indexcols + svaluecols + [cols["product"] + "_s"]
dfc = dfc[usecols]
# %%
df = df.merge(dfc, on=indexcols, how="left")


#%% Replace the values and remove the columns _s
for vc in value_cols:
    _ = df.loc[~df[vc + "_s"].isnull(), vc] = df[vc + "_s"]
df.drop(columns=svaluecols, inplace=True)
#%%
df.loc[~df[cols["product"] + "_s"].isnull(), cols["product"]] = df[cols["product"] + "_s"]
#%% Save
#collist = [cols[x] for x in cols]  + value_cols + [cols["postcode"] + "_count"]
mypath = os.path.join(mydir, out_file)
df.to_csv(mypath, index=None)
print(f"Output is saved")


# %%
