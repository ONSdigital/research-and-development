# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 17:19:57 2023

@author: zoring
"""

import pandas as pd

usecols = ["2016 > Pub PG", "2016 > Form PG"]
names = ["pg_alpha",  "pg_numeric"]

map_df = pd.read_csv(
        r"R:\BERD Results System Development 2023\DAP_emulation\mappers\SIC_to_PG_UTF-8.csv",
        usecols=usecols,
       )
map_df.columns = names
#%%
df = map_df.groupby(["pg_alpha"]).agg({'pg_alpha': 'min', 'pg_numeric': 'min'}) 
  
#%%
#df = map_df.drop_duplicates()
df.to_csv(
        r"R:\BERD Results System Development 2023\DAP_emulation\mappers\pg_alpha_num.csv",
        index=None
        )