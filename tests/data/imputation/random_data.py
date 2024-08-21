import pandas as pd
import numpy as np

# Read the CSV file into a pandas DataFrame
import os

# Get the absolute path of the script
script_path = os.path.dirname(os.path.abspath(__file__))

# Construct the absolute path of the CSV file
csv_file_path = "D:/coding_projects/randd_test_data/imp_CD_E.csv"

backdata_path = "R:/BERD Results System Development 2023/DAP_emulation/2022_surveys/BERD/06_imputation/backdata_output/2022_backdata_published_v347.csv"

# Read the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file_path)

back_df = pd.read_csv(backdata_path)

back_df = back_df.loc[back_df["201"].isin(["AD", "E"])]

rename_refs = {ref: 1000 + i for i, ref in enumerate(df["reference"].unique())}

# only keep backdata rows where the "reference" is also in df
back_df = back_df.loc[back_df["reference"].isin(rename_refs.keys())]

# Replace the values in the "reference" column with the new values
df["reference"] = df["reference"].replace(rename_refs)

# Repeat for backdata
back_df["reference"] = back_df["reference"].replace(rename_refs)

# Randomize the specified columns
# for column in columns_to_randomize:
#     df[column] = np.random.permutation(df[column].values)

# Display the DataFrame to verify the changes
print(df)

# Save the DataFrame to a new CSV file in the same location as the original file
output_csv_file_path = os.path.join(script_path, 'imp_AD_E_rand.csv')
back_csv_file_path = os.path.join(script_path, 'backdata_AD_E_rand.csv')
df.to_csv(output_csv_file_path, index=False)
back_df.to_csv(back_csv_file_path, index=False)
