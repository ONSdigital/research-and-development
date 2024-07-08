'''This script produces a synthetic small sample of the anonymised SPP snapshot.
It also creates a postcodes mapper that only has the postcodes we need.'''

import pandas as pd
from os.path import join

# Configuration hardcoded - input arguments
config  = {
    "global": {
        "create_schemas": True,
    },
    "network_paths": {
        "input_dir": r"D:\data\res_dev\synthetic\inputs",
        "output_dir": r"D:\data\res_dev\synthetic\outputs",
        "schemas_dir": r"./config/synthetic_schemas",
        "input_snapshot": r"staged_BERD_full_responses_2024-06-03_v16.csv",
    },
    ,
    "hdfs_paths": {
        "input_dir": "ons/rdbe_dev/synthetic/inputs",
        "output_dir": "ons/rdbe_dev/synthetic/outputs",
        "schemas_dir": "./config/synthetic_schemas",
        "input_snapshot": r"staged_BERD_full_responses_2024-06-03_v16.csv",
    },
}
}

# Create schema of staged df
def save_schema(df: pd.DataFrame, shema_path: str) -> None: 
    """Creates a dictionary of column names and data types and saves it in
    a text file following the format of the existing toml data dictionaries.

    Args:
        df (pd.DataFrame): The dataframe for which the schema needs to be saved.
        shema_path (str): Full path. Shoud be an r-string in windows. Can be 
        anything, but it's better if it has .toml extension

    Returns:
        None
    """
    # Get column names  as data types as dict of strings
    column_types = dict(df.dtypes)
    schema_dict = {col: str(column_types[col]) for col in column_types}

    # Calculate the schema
    # Create an empty string to initialise it
    schema_str = ""

    # Iterate through columns
    for col in schema_dict:
        s = f'[{col}]\nold_name = "{col}"\nDeduced_Data_Type = "{schema_dict[col]}"\n'
        
        # Adding a blank line after the first existing non-empty entry
        if schema_str != '':
            schema_str = schema_str + '\n'

        # Adding a new entry
        schema_str = schema_str + s

    # Output the schema toml file
    text_file = open(shema_path, "w")
    text_file.write(schema_str)
    text_file.close()

    return None


# Create schemas of contributors and responders

# Read input file 

# Sample input df

# Split input df into two


# Apply schemas of two dfs

# Convert two dfs into dictionaries

# Combine two dictionaries in one

# Save dictionary into json

# Read postcodes

# Sample postcodes df using sampled input df

# Save sampled poscodes with schema

# Report success

# Create contributors and ??? schemas

# Report success

# Run everything
if __name__ == "__main__":
    df = pd.DataFrame({
        "A": ["a", "b", "c"],
        "B": [1, 2, 3],
    })

    mydir = config["network_paths"]["schemas_dir"]
    myfile = "test_schema.toml"
    mypath = join(mydir, myfile)
    save_schema(df, mypath)
    print("Synthetic files created successfully")