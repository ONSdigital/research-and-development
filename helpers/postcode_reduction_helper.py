"""Read in the large postcode lookup and save smaller csv with two required columns."""

import pandas as pd

# configuration settings
survey_year = 2020
# Input folder and file names
root_fol = "R:/BERD Results System Development 2023/DAP_emulation/"
in_fol = root_fol + "ONS_Postcode_Reference/"
in_file = "postcodes_pcd2_itl.csv"

# Output folder and file names
out_fol = root_fol + f"{survey_year}_surveys/mappers/"
out_file = "postcodes_mapper_reduced.csv"

key_cols = ["pcd2", "itl"]

# validate the inputs
# test the survey year is a valid year as an integer
if not ((survey_year > 2020) and (survey_year < 2035)):
    msg = f"""The survey_year value {survey_year} is not valid.
           Enter an integer between 2021 and 2034"""
    raise Exception(msg)

# check the input and output paths are valid

# Read in files

in_path = in_fol + in_file
with open(in_path, "r") as file:
    # Import csv file and convert to Dataframe
    try:
        df = pd.read_csv(file, usecols=key_cols, low_memory=False)
    except Exception:
        print(f"Could not find specified columns in {in_path}")
        print("Columns specified: " + str(key_cols))
        raise ValueError

print(df.head())
# save output
out_path = out_fol + out_file
with open(out_path, "w", newline="\n", encoding="utf-8") as file:
    # Write dataframe to the file
    df.to_csv(file, index=False)
print("file saved to " + out_fol)
