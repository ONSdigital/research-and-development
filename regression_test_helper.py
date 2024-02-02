"""Read in a csv file and ouput a test file with data for a unit test."""
import pandas as pd
import os

# configuration settings
csv_path = "D:/coding_projects/randd_test_data/"
input_file1 = "test.csv"

# whether the unit test data is input or expected output
in_or_output = "input"

output_filename = f"{in_or_output}_function"

# read in the csv
path1 = os.path.join(csv_path, input_file)
df1 = pd.read_csv(path1)


# set all datatypes to string - we are outputting all the data as a string
df1 = df1.astype(str)

# add quotes to the strings in the columns that should show as string types
string_cols = [] # ["period"]

df1[string_cols] = df1[string_cols].applymap('"{}"'.format)

# prepare the output formatting
tab = " "*4

col_list = df1.columns
col_string = ""

# create a new column that joins the contents of the other columns
df1['output'] = f"{tab}["
for col in df1.columns[:-1]:
    df1["output"] += df1[col] + ", "
    col_string += f'{tab}{tab}"{col}",\n'
    
df1['output'] += df1[df1.columns[-2]] + "],"

# concatenate everything in the new column into a single string
rows_string = df1["output"].str.cat(sep=f"\n{tab}")

# join all the components into a final string for output
full_text = f'''def create_input_df(self):
    """Create an input dataframe for the test."""
    {in_or_output}_columns = [\n{col_string}{tab}]
        
    data = [\n{tab}{rows_string}\n{tab}]   

    {in_or_output}_df = pandasDF(data=data, columns={in_or_output}_columns)
    return {in_or_output}_df
    '''

# write the prepared text to a txt file
out_path = os.path.join(csv_path, output_filename + ".txt")

text_file = open(out_path, "w")
text_file.write(full_text)
text_file.close()
