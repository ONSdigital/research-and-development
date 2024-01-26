# -*- coding: utf-8 -*-
"""
Created on Fri Jan 26 10:54:25 2024
Comparing alternative ways of postcode ti ITL1 mapping
Existing way:
    merge postcodes to main dataset on a condition postcodes_harmonised==postcodes.pcd2
    in poostcodes mapper, we map pcd2 to itl (many to one)
    merge itl  mapper on a contiion postcodes.itl==itl.LAU121CD
    use ITL121CD
alternative way:
    merge postcodes to main dataset on a condition postcodes_harmonised==postcodes.pcd2
    in poostcodes mapper, we map pcd2 to oslaua (many to one)
    merge itl  mapper on a contiion postcodes.oslaua==itl.LAU121CD
    use ITL121CD

This script will produce two alternative coulumns of itl1 codes: itl1_itl and 
itl1_oslaua for comparison
@author: zoring
"""
import pandas as pd
from src.staging.staging_helpers import postcode_topup


#%%
df_path = r"D:\data\res_dev\staged_BERD_full_responses.csv"
postcodes_path = r"D:\data\res_dev\postcodes_full_head.csv"
itl_path = r"D:\data\res_dev\itl_gz.csv"

#%% Load staged full responses
df_cols = ["601", "referencepostcode" ]
df = pd.read_csv(df_path, usecols=df_cols)

#%% Put all codes in one column and harmonise
df_601 = pd.DataFrame({"postcode": []})
df_601["postcode"] = df["601"]

df_ref = pd.DataFrame({"postcode": []})
df_ref["postcode"] = df["referencepostcode"]

#%% Drop duplicates
df = pd.concat([df_601, df_ref]).drop_duplicates()

#%% Harmonise
df["postcode"]= df["postcode"].apply(postcode_topup)

#%% Load postcodes
pc_cols = ["pcd2", "oslaua", "itl"]
df_pc = pd.read_csv(postcodes_path, usecols=pc_cols)

#%% Load itl
itl_cols=["LAU121CD", ]
df_itl = pd.read_csv(itl_path)

#%% Merge postcodes to df
df = df.merge(df_pc, how="left", left_on="postcode", right_on="pcd2")

#%% Join itl traditional way
mapper = df_itl[["]]
