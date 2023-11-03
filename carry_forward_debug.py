# -*- coding: utf-8 -*-
"""
Created on Fri Nov  3 13:34:57 2023
Carry forward - amnalysis
@author: zoring
"""
import pandas as pd
        
df = pd.read_pickle(r"d:\data\df.pkl")
backdata = pd.read_pickle(r"d:\data\backdata.pkl")

#%%

df = pd.merge(df,
              backdata,
              how="left",
              on="reference",
              suffixes=("", "_prev"),
              indicator=True)
#%%
df.to_csv(r"d:\data\after_merge.csv", index=None)
#%% keep only the rows needed, see function docstring for details.
no_match_cond = df["_merge"] == "left_only"
instance_cond = ((df["instance"] == 0) | pd.isnull(df["instance"]))
keep_cond = no_match_cond | instance_cond

df = df.copy().loc[keep_cond, :]
df.to_csv(r"d:\data\keep_cond.csv", index=None)

# Copy values from relevant columns where references match
match_cond = df["_merge"] == "both"

# replace the values of certain columns with the values from the back data
# TODO: Check with methodology or BAU as to which other cols to take from backdata
# TODO: By default, columns not updated such as 4xx, 5xx, 6xx
# TODO: will contain the current data, instance 0.
replace_vars = ["instance", "200", "201"]
for var in replace_vars:
    df.loc[match_cond, var] = df.loc[match_cond, f"{var}_prev"]
impute_vars = ["emp_researcher_imputed", "emp_technician_imputed", "emp_other_imputed"]

for var in impute_vars:
    df.loc[match_cond, f"{var}_imputed"] = df.loc[match_cond, f"{var}_prev"]
df.loc[match_cond, "imp_marker"] = "CF"

# Drop merge related columns
to_drop = [column for column in df.columns if column.endswith('_prev')]
to_drop += ['_merge']
df = df.drop(to_drop, axis=1)


