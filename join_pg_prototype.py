# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 17:40:21 2023

@author: zoring
"""

import src.outputs.map_output_cols as map_o
import pandas as pd

  # debugging begin
mapper_df = pd.read_csv(r"R:\BERD Results System Development 2023\DAP_emulation\mappers\pg_alpha_num.csv")
df = pd.read_csv(r"D:\data\res_dev\outputs\tau_output_raw.csv")
df_out = map_o.join_pg_numeric(df, mapper_df, cols_pg=["201"])
