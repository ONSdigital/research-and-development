# -*- coding: utf-8 -*-
"""
Created on Thu Nov 16 10:54:27 2023
Create schema
@author: zoring
"""
import pandas as pd
import os

# Imputation file location and name
common_dir = "R:\\BERD Results System Development 2023\\DAP_emulation\\"
output_dir = "imputation\\imputation_qa"

pref = "full_responses_imputed"
suff = "_2023-11-16_v340.csv"

# Output folder for all schemas
out_dir = r"config\output_schemas"

# Read the top 10 rows, inferrring the schema from csv
mydir = common_dir + output_dir
mypath = os.path.join(mydir, pref + suff)
df = pd.read_csv(mypath, nrows=10)
df = df.drop("index", axis=1)

# Get column names  as data types as dict of strings
types = dict(df.dtypes)
schema = {col: str(types[col]) for col in types}

# Calculate the schema

# Initially, the schema is empty
S = ""

# Iterate through columns
for col in schema:
    s = f'[{col}]\nold_name = "{col}"\nDeduced_Data_Type = "{schema[col]}"\n\n'
    S = S + s

# Output the schema toml file
mypath = os.path.join(out_dir, pref + '_schema.toml')
text_file = open(mypath, "w")
text_file.write(S)
text_file.close()
