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
import os

mydir = r"D:\repos\research-and-development"
os.chdir(mydir)

print(os.getcwd())
#%%
from src.staging.staging_helpers import postcode_topup


#%%
df_path = r"D:\data\res_dev\full_estimation_qa_2024-01-17_v210.csv"
postcodes_path = r"D:\data\res_dev\ONSPD_NOV_2022_UK.csv"
itl_path = r"R:\BERD Results System Development 2023\DAP_emulation\mappers\2023\itl_61_62.csv"

#%% Prepare df
df_cols = ["601", "referencepostcode" ]
df = pd.read_csv(df_path, usecols=df_cols)

#%% Put all codes in one column and harmonise
df_601 = pd.DataFrame({"postcode": []})
df_601["postcode"] = df["601"]

df_ref = pd.DataFrame({"postcode": []})
df_ref["postcode"] = df["referencepostcode"]

#%% Drop duplicates
df = pd.concat([df_601, df_ref]).drop_duplicates()

# Remove nulls
df=df.loc[df["postcode"].notnull()]
df = df.loc[df["postcode"].str.len() > 0]



#%% Harmonise
df["postcode"]= df["postcode"].apply(postcode_topup)
df = df.loc[~df["postcode"].str.startswith(" ")]

#%% Sort postcodes alphabetically
df.sort_values(by=['postcode'], inplace=True)

df = df.drop_duplicates()

# Reset index
df = df.reset_index()[["postcode"]]
#%%
#%% Load postcodes
pc_cols = ["pcd2", "oslaua", "itl", "rgn"]
df_pc = pd.read_csv(postcodes_path, usecols=pc_cols)

#%% Summary of missing postcodes.itl that cannot be matched to LAU121CD
df_missing = df_pc.loc[df_pc["itl"].isin(["E06000061", "E06000062"])]

df_m = df_missing[["itl", "rgn"]].drop_duplicates()

#%% Other postcodes and itls in the rgn code E12000004

df_other =df_pc.loc[df_pc["rgn"].isin(["E12000004"])][["itl", "rgn"]].drop_duplicates().sort_values(by="itl")


#%% Load itl
itl_cols=["LAD20CD", "LAU121CD", "ITL121CD"]
df_itl = pd.read_csv(itl_path, usecols=itl_cols)

#%% Merge postcodes to df
df_pcd2 = df.merge(df_pc, how="left", left_on="postcode", right_on="pcd2")

#%% known postcodes
df_known = df_pcd2.loc[df_pcd2["itl"].isin(["E06000061", "E06000062"])][["postcode", "itl"]].drop_duplicates().sort_values(by=["itl","postcode"])


#%% Join itl traditional way
df_itl1 = df_pcd2.merge(df_itl, how="left", left_on="itl", right_on="LAU121CD")
#df.rename(columns={"ITL121CD": "itl_old"}, inplace=True)

#%% Join the new way
#==============================================================================
# df = df.merge(df_itl, how="left", left_on="oslaua", right_on="LAU121CD")
# df.rename(columns={"ITL121CD": "itl_new"}, inplace=True)
# 
# #%% Create Boolean
# df["itls_equal"] = df["itl_new"] == df["itl_old"] 
#==============================================================================
#%% Save
#mypath = r"D:\data\res_dev\postcodes_alternative.csv"
#df.to_csv(mypath, index=None)
