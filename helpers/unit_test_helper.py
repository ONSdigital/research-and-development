"""Read in a csv file and ouput a test file with data for a unit test."""
import pandas as pd
import os

# configuration settings
csv_path = "D:/coding_projects/randd_test_data/"
input_file = "percent_test.csv"

# whether the unit test data is input or expected output (set "input" or "exp_output")
in_or_output = "exp_output"

output_filename = f"{in_or_output}_function"

# read in the csv
path1 = os.path.join(csv_path, input_file)
df1 = pd.read_csv(path1)

# specify string columns- these will have quotes applied
string_cols = ["formtype", "601", "status", "postcodes_harmonised"]

# specify float columns
float_cols = ["601_count"]

# set all datatypes to string - we are outputting all the data as a string
df1 = df1.astype(str)

# if formtype is in the columns, convert to 0001 and 0006
if "formtype" in df1.columns:
    df1.formtype = df1.formtype.str.zfill(4)


def add_quotes(x):
    """add quotes to the strings in the columns that should show as string types."""
    x = '"' + x + '"'
    return x

def format_float(x):
    """Add a point and a zero for columns that should show as floats."""
    if "." in x:
        return x
    else:
        return x + ".0"

for c in string_cols:
    df1.loc[df1[c] != "nan", c] = df1.loc[df1[c] != "nan", c].apply(add_quotes, 1)

for c in float_cols:
    df1.loc[df1[c] != "nan", c] = df1.loc[df1[c] != "nan", c].apply(format_float, 1)

# replace nulls, which are now  rendered "nan" since we made all columns strings,
# with "np.nan" for output
df1 = df1.replace("nan", "np.nan")

# prepare the output formatting
tab = " "*4

# create the text to represent a list of the columns
col_list = df1.columns
col_string = ""
for col in df1.columns:
    col_string += f'{tab}{tab}"{col}",\n'

# create a new column that joins the contents of the other columns
df1['output'] = f"{tab}["
for col in df1.columns[:-2]:
    df1["output"] += df1[col] + ", "

df1['output'] += df1[df1.columns[-2]] + "],"

# concatenate everything in the new column into a single string
rows_string = df1["output"].str.cat(sep=f"\n{tab}")

# join all the components into a final string for output
full_text = f'''def create_{in_or_output}_df(self):
    """Create an {in_or_output} dataframe for the test."""
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
