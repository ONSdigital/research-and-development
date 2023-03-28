import os

import pandas as pd

import pydoop.hdfs as hdfs
import csv

# create a pandas.DataFrame
sales = {"account": ["Jones LLC", "Alpha Co", "Blue Inc"], "Jan": [150, 200, 50]}
data = pd.DataFrame.from_dict(sales)
newsales = {"account": ["Ilyas"], "Jan": [1000]}
newdata = pd.DataFrame.from_dict(newsales)
context = os.getenv("HADOOP_USER_NAME")  # Put your context name here
project = "alii3_rdbe"  # Put your project name here

main_path = f"/user/{context}/{project}"

file_path = f"{main_path}/testing_headers.csv"
# with hdfs.open(file_path, "r") as file:
#     df_imported_from_hdfs = pd.read_csv(file)
#     d2 = df_imported_from_hdfs.append(data)
main_columns = ["run_id", "timestamp", "version", "duration"]
main_col = pd.DataFrame(main_columns)
# with hdfs.open(file_path, "wt") as file:
#     writer = csv.writer(file)
#     writer.writerow(main_columns)
#     # main_col.to_csv(file, index=False, header=True)
newrow = {
    "run_id": 900,
    "timestamp": "28/03/2023-15:42:58",
    "version": "0.1.0",
    "duration": "0.07413",
}
new = pd.DataFrame(
    [newrow],
    columns=["run_id", "timestamp", "version", "duration"],
)
if not hdfs.path.isfile(file_path):
    with hdfs.open(file_path, "wt") as file:
        writer = csv.writer(file)
        writer.writerow(main_columns)

with hdfs.open(file_path, "r") as file:
    df_imported_from_hdfs = pd.read_csv(file)
    print(df_imported_from_hdfs)
    newdata = df_imported_from_hdfs.append(new)
    print(newdata)

with hdfs.open(file_path, "wt") as file:
    newdata.to_csv(file, index=False)
