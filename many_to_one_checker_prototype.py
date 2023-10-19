# -*- coding: utf-8 -*-
"""
Created on Wed Oct 18 10:02:28 2023
many_to_one checker
@author: zoring
"""
import pandas as pd
import os
dir_in = r"R:\BERD Results System Development 2023\DAP_emulation\mappers"
file_in = "sic_to_pg_alpha.csv"

cols ={
       "sic": "sic",
       "pg_alpha": "pg_alpha"}

#%% Read, rename, de-duplicate
infile = os.path.join(dir_in, file_in)
usecols = [cols[x] for x in cols]
names = {cols[x]: x for x in cols}

df = pd.read_csv(infile, usecols=usecols).rename(columns=names).drop_duplicates()

#%%
col_one = "pg_alpha"
col_many = "sic"

df_agg = df[[col_one, col_many]].drop_duplicates()

df_count = (df_agg
            .groupby(col_many)
            .agg({col_one:'count'})
            .reset_index()
            .rename(columns={col_one:'code_count'}))

outfile = os.path.join(
        dir_in, f"Counts {col_many} to {col_one}.csv")
df_count.to_csv(outfile, index=None)

