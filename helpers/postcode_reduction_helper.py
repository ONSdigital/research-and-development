"""Read in the large postcode lookup and save smaller csv with two required columns."""

import pandas as pd
import os
import sys

sys.path.append("D:/coding_projects/github_repos/research-and-development")  # noqa

from src.utils.config import config_setup  # noqa
from src.utils.local_file_mods import rd_file_exists  # noqa

# load config
dev_config_path = os.path.join("src", "dev_config.yaml")
user_config_path = os.path.join("src", "user_config.yaml")
config = config_setup(user_config_path, dev_config_path)

survey_year = config["global"]["survey_year"]
# test the survey year is a valid year as an integer
if not ((survey_year > 2021) and (survey_year < 2041)):
    msg = f"""The survey_year value {survey_year} is not valid.
           Enter an integer between 2022 and 2040"""
    raise Exception(msg)

# Input and output folder and file names
in_file = config["network_paths"]["postcode_masterlist"]
out_fol = config["network_paths"]["mapper_path"]
# check the input paths are valid
rd_file_exists(in_file)

# Output folder and file names
out_path = out_fol + f"postcodes_{survey_year}.csv"

# Read in files
key_cols = ["pcd2", "itl"]
with open(in_file, "r") as file:
    # Import csv file and convert to Dataframe
    try:
        df = pd.read_csv(file, usecols=key_cols, low_memory=False)
    except Exception:
        print(f"Could not find specified columns in {in_file}")
        print("Columns specified: " + str(key_cols))
        raise ValueError

# write the reduced file to the new destination
with open(out_path, "w", newline="\n", encoding="utf-8") as file:
    # Write dataframe to the file
    df.to_csv(file, index=False)
print("file saved to " + out_fol)
