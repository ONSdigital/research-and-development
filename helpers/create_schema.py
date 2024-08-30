# -*- coding: utf-8 -*-
"""
Created on Thu Nov 16 10:54:27 2023
Create schema
@author: zoring
"""
import pandas as pd
import os

# Imputation file location and name
root = "R:/BERD Results System Development 2023/DAP_emulation/"
input_dir = "2023_surveys/BERD/06_imputation/imputation_qa/"

output_name = "wrong_604_error_qa"
year = 2023
suff = "24-08-30_v737.csv"

# Output folder for all schemas
out_dir = r"config\output_schemas"

# Read the top 10 rows, inferrring the schema from csv
mypath = os.path.join(root, input_dir, f"{year}_{output_name}_{suff}")

# check the file exists
if not os.path.exists(mypath):
    raise FileNotFoundError(f"File not found: {mypath}")

df = pd.read_csv(mypath, nrows=10)

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
mypath = os.path.join(out_dir, output_name + "_schema.toml")
text_file = open(mypath, "w")
text_file.write(S)
text_file.close()
