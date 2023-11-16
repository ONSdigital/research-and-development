# -*- coding: utf-8 -*-
"""
Created on Thu Nov 16 10:54:27 2023
Create schema
@author: zoring
"""
import pandas as pd
import os
#%% Estimation
#mydir = r"R:\BERD Results System Development 2023\DAP_emulation\estimation\estimation_qa"
#pref = "full_estimation_qa"
#suff = "_2023-11-02_v57.csv"

#%% Imputation


mydir = r"R:\BERD Results System Development 2023\DAP_emulation\imputation\imputation_qa"
pref = "full_responses_imputed"
suff = "_2023-11-16_v340.csv"

out_fol = r"config\output_schemas"
#%% Read the ent
mypath = os.path.join(mydir, pref + suff)
df = pd.read_csv(mypath, nrows=10)

#%% 
types = dict(df.dtypes)
schema = {col: str(types[col]) for col in types}

#%%

S = ""

for col in schema:
    s = (
            f'[{col}]' + '\n' + 
            f'old_name = "{col}"' + '\n' +
            f'Deduced_Data_Type = "{schema[col]}"\n\n'
            )
    S = S + s
 
#%%
mypath = os.path.join(out_fol, pref + '_schema.toml')
text_file = open(mypath, "w")
text_file.write(S)
text_file.close()

