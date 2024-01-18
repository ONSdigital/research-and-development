# -*- coding: utf-8 -*-
"""
Created on Thu Jan 18 12:38:51 2024
compare non-ni and ni
@author: zoring
"""
import pandas as pd
import os

mydir = r"R:\BERD Results System Development 2023\DAP_emulation\outputs\output_gb_sas"
file_ni = "output_gb_sas_2024-01-18_v94.csv"
file_gb = "output_gb_sas_2024-01-18_v95.csv"

#%%
mypath = os.path.join(mydir, file_ni)
df_ni = pd.read_csv(mypath, usecols=["ref"], dtype={'ref': "str"})

#%%
mypath = os.path.join(mydir, file_gb)
df_gb = pd.read_csv(mypath, usecols=["ref"], dtype={'ref': "str"})

#%%
df = df_gb.merge(df_ni, left_on="ref", right_on="ref", how="outer", indicator=True)

#%%
df_x = df[df["_merge"]!="both"]