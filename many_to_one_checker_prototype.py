# -*- coding: utf-8 -*-
"""
Created on Wed Oct 18 10:02:28 2023
many_to_one checker
@author: zoring
"""
import pandas as pd
import os
dir_in = r"R:\BERD Results System Development 2023\DAP_emulation\mappers\2023"
file_in = "SIC_to_PG.csv"

cols ={
       "sic": "SIC 2007_CODE",
       "pg_num": "2016 > Form PG",
       "pg_alpha": "2016 > Pub PG"}

#%% Read, rename, de-duplicate
infile = os.path.join(dir_in, file_in)
usecols = [cols[x] for x in cols]
names = {cols[x]: x for x in cols}

df = pd.read_csv(infile, usecols=usecols).rename(columns=names).drop_duplicates()

#%%
col_one = "pg_alpha"
col_many = "pg_num"

df_agg = df[[col_one, col_many]].drop_duplicates()

df_count = (df_agg
            .groupby(col_many)
            .agg({col_one:'count'})
            .reset_index()
            .rename(columns={col_one:'code_count'}))

outfile = os.path.join(
        dir_in, f"Counts {col_many} to {col_one}.csv")
#df_count.to_csv(outfile, index=None)

#%%

dftest=df_agg[[col_many, col_one]]
myzip = zip(df_agg[col_many], df_agg[col_one])
map_dict = dict(myzip)


#%%
from src.staging.pg_conversion import pg_to_pg_mapper

print( "PG mapper function importd")

# mydf = pg_to_pg_mapper(
#         df, 
#         mapper=df_agg,
#         from_col="pg_num",
#         to_col="pg_alpha")
