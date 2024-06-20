"""Read in the large postcode lookup and save smaller csv with two required columns."""

import pandas as pd
import os
import sys

sys.path.append("D:/coding_projects/github_repos/research-and-development")  # noqa

from src.utils.helpers import Config_settings  # noqa
from src.utils.local_file_mods import check_file_exists  # noqa

# load config
config_path = os.path.join("src", "developer_config.yaml")
conf_obj = Config_settings(config_path)
config = conf_obj.config_dict

survey_year = config["global"]["survey_year"]
# test the survey year is a valid year as an integer
if not ((survey_year > 2021) and (survey_year < 2041)):
    msg = f"""The survey_year value {survey_year} is not valid.
           Enter an integer between 2022 and 2040"""
    raise Exception(msg)

# Input folder and file names
in_file = config["network_paths"]["postcode_masterlist"]

# Output folder and file names
root_fol = "R:/BERD Results System Development 2023/DAP_emulation/"
out_fol = root_fol + f"{survey_year}_surveys/mappers/"
out_path = out_fol + "postcodes_mapper_reduced.csv"

# check the input paths are valid
check_file_exists(in_file)

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
