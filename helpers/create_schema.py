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
input_dir = "2022_surveys/BERD/01_staging/staging_qa/postcode_validation/"

# test = "R:\BERD Results System Development 2023\DAP_emulation\2022_surveys\BERD\10_outputs\output_status_filtered_qa\2022_status_filtered_qa_24-07-05_v505.csv"

output_name = "invalid_unrecognised_postcodes"
year = 2022
suff = "24-07-04_v503.csv"

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
