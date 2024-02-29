# -*- coding: utf-8 -*-
"""
Created on Thu Feb 29 12:15:06 2024

@author: zoring
"""

import pandas as pd

dat = {"A": [None, pd.NA], "B":[0, 0]}

df=pd.DataFrame(dat)

blank_cols = ["C", "D"]
blank_values = [pd.NA for x in blank_cols]
zero_cols = ["E", "F"]
zero_values = [0 for x in zero_cols]

df = df.reindex(columns=df.columns.tolist() + blank_cols + zero_cols)   # add empty cols
df[blank_cols] = blank_values  # multi-column assignment works for existing cols
df[zero_cols] = zero_values  # multi-column assignment works for existing cols

df.to_csv(r"D:\work\data\res_dev\null_test.csv", index=None)

print(dict(df.dtypes))