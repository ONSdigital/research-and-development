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
pecent_col = "602"
form_col = "formtype"
short_code = "0006"
long_code = "0001"

short_percent = 100.0
value_cols = ["407", "408", "409"]

#%%
mypath = os.path.join(mydir, in_file)
df = pd.read_pickle(mypath)
print(f"Input df is read. Columns are:\n{df.dtypes}")

#%% Assign 100 to short forms
cond = df[form_col] == short_code
df_out = df.copy()
cond = df_out[form_col] == short_code
df_out[pecent_col].mask(cond, other=short_percent, inplace=True)


#%%
#cond = (
#        (df[form_col] == long_code)
#        )
#dfl = df[cond].copy()
#dfl[pecent_col].fillna(0, inplace=True)
#
##%% Get the sum of weigths per grop
#group_cols = [period_col, ref_col]
#dfl["percent_total"] = dfl.groupby(group_cols)[pecent_col].transform(sum)
#
##%% Apply weights when it's 100
#df100 = dfl[dfl["percent_total"] == 100]
#df0 = dfl[dfl["percent_total"] == 0]
#df50 = dfl[(dfl["percent_total"] > 0) & (dfl["percent_total"] < 100)]

#%%
mypath = os.path.join(mydir, out_file)
df_out.to_csv(mypath, index=None)
print(f"Output is saved")

