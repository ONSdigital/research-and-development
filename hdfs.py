import os

import pandas as pd

import pydoop.hdfs as hdfs


# create a pandas.DataFrame
sales = {"account": ["Jones LLC", "Alpha Co", "Blue Inc"], "Jan": [150, 200, 50]}
data = pd.DataFrame.from_dict(sales)

context = os.getenv("HADOOP_USER_NAME")  # Put your context name here
project = "alii3_rdbe"  # Put your project name here

main_path = f"/user/{context}/{project}"

file_path = f"{main_path}/test_export_using_pydoop.csv"
with hdfs.open(file_path, "wt") as file:
    data.to_csv(file, index=False, header=False, mode="a")
