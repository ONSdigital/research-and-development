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
df_path = r"R:\BERD Results System Development 2023\DAP_emulation\staging\staging_qa\full_responses_qa\staged_BERD_full_responses_2024-01-05_v84.csv"
postcodes_path = r"D:\data\res_dev\ONSPD_NOV_2022_UK.csv"
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
df = pd.concat([df_601, df_ref]).drop_duplicates().reset_index()

#%% Harmonise
df["postcode"]= df["postcode"].apply(postcode_topup).drop_duplicates()

#%% remove blanks
df = df[df["postcode"].notnull()]

#%% Load postcodes
pc_cols = ["pcd2", "oslaua", "itl"]
df_pc = pd.read_csv(postcodes_path, usecols=pc_cols)

#%% Load itl
itl_cols=["LAD20CD", "LAU121CD", "ITL121CD"]
df_itl = pd.read_csv(itl_path, usecols=itl_cols)

#%% Merge postcodes to df
df = df.merge(df_pc, how="left", left_on="postcode", right_on="pcd2")

#%% Join itl traditional way
df = df.merge(df_itl, how="left", left_on="itl", right_on="LAD20CD")
df.rename(columns={"ITL121CD": "itl_old"}, inplace=True)

#%% Join the new way
df = df.merge(df_itl, how="left", left_on="oslaua", right_on="LAU121CD")
df.rename(columns={"ITL121CD": "itl_new"}, inplace=True)

#%% Create Boolean
df["itls_equal"] = df["itl_new"] == df["itl_old"] 
#%% Save
mypath = r"D:\data\res_dev\postcodes_alternative.csv"
df.to_csv(mypath, index=None)
