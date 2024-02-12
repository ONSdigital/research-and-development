# -*- coding: utf-8 -*-
"""
Created on Thu Feb  8 13:26:30 2024
Counting unique keys prototyoe
@author: zoring

"""
#%%
import pandas as pd

#%%
mydata = {
    "ref": [1, 2, 2],
    "200": ["C", "C", "D"],
    "201": ["A", "A", pd.NA],
    "601": ["CF", "CF", "BS"],
    "251": ["yes", "yes", "no" ]       
}

#%%
df = pd.DataFrame(mydata)

key_cols = ["ref"]
product_cols = ["200", "201"]
postcode_cols = ["601"]
textual_cols = ["251"]

product_count_col = "product_count"


#%%
def count_codes(df, key_cols, code_cols, new_name):
    df_count = df[key_cols + code_cols]
    
    #% remove nulls
    for col in code_cols:
        df_count = df_count.loc[df_count[col].notnull()]
        
    #% de-duplicate
    df_count = df_count.drop_duplicates()
    
    #% Agregate abd count
    df_agg = df_count.groupby(key_cols).agg("count").reset_index()
    keep_col = code_cols[0]
    df_agg = df_agg[key_cols + [keep_col]]
    df_agg.rename(columns={keep_col: new_name}, inplace=True) 
    
    #%merge back
    df = df.merge(
        df_agg,
        on=key_cols,
        how="left"
    )
    return df

#%%
df = count_codes(df, key_cols, product_cols, "product_count")
df = count_codes(df, key_cols, postcode_cols, "postcode_count")
df = count_codes(df, key_cols, textual_cols, "textual_count")

n = df.shape[1]

