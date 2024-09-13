# -*- coding: utf-8 -*-
"""
Created on Wed Sep 11 13:50:53 2024
This script extracts numeric pg codes that map to more than one pg_alpha, 
depending on the sic code
@author: zoring
"""
#%% User-defined paths
fol_in = r"R:\BERD Results System Development 2023\DAP_emulation\mappers"
map_in = "SIC_to_PG.csv"
sic_col = "SIC 2007_CODE"
alpha_col = "2016 > Pub PG"
num_col = "2016 > Form PG"

fol_out = r"R:\BERD Results System Development 2023\DAP_emulation\analysis\pg_consistency"
map_out = "SIC_to_PG_extract_gz_2024_09_11_v01.csv"

#%% Import libraries
from os.path import join
import pandas as pd

#%% Load input
mypath = join(fol_in, map_in) 
df = pd.read_csv(
    mypath, 
    usecols=[sic_col, alpha_col, num_col]
).drop_duplicates()

#%% Identify numeric codes that map to more than one alpha codes
df_agg = df[[alpha_col, num_col]].drop_duplicates().groupby(num_col).agg("count").reset_index()
df_multy = df_agg.loc[df_agg[alpha_col] > 1][[num_col]]
multi_list = list(df_multy[num_col])

#%% Extract potential multiplicate 
df_out = df.loc[df[num_col].isin(multi_list)]
mypath = join(fol_out, map_out)
df_out.to_csv(mypath, index=None)
