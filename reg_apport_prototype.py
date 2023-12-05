# -*- coding: utf-8 -*-
"""
Created on Wed Nov 22 11:46:52 2023
harmonise lengths
@author: zoring

"""
import pandas as pd
import os
#%% Loading ddata
mydir = r"D:\data\res_dev\outputs\reg_apport"
in_file = "outputs_df_before.pkl"
out_file = "outputs_df_corrected.csv"

#%% Parameters
ref_col = "reference"
ins_col = "instance"
period_col = "period"
short_code = "0006"
long_code = "0001"

short_percent = 100.0
value_cols = ["407", "408", "409"]

# %%  Functions
def short_percent(df):
    form_col = "formtype"
    pecent_col = "602"
    short_percent = 100.0
    cond = df[form_col] == short_code
    return df[pecent_col].mask(cond, other=short_percent, inplace=True)


#%%
mypath = os.path.join(mydir, in_file)
df = pd.read_pickle(mypath)
print(f"Input df is read. Columns are:\n{df.dtypes}")

#%% Cases
cases = {}

#%% Case A Assign 100 to short forms
cases.update({"short form": short_percent})

#df_out = short_percent(df)

#%%

cond = cond = df[form_col] == long_code

df_l = df.copy()[cond]
#%%
dfg = df_l.groupby([ref_col, period_col])

#%%
mypath = os.path.join(mydir, out_file)
#df_out.to_csv(mypath, index=None)
print(f"Output is saved")

