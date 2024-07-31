# To test the ability to read, write and check if the file exists in s3 file system

# user parameters
mydir = "/user/george.zorinyants/res_dev/2022_surveys/mappers/v1"
myfile = "pg_num_alpha_2022.csv"

# load librarioes
import pandas as pd
import boto3
import io

# Read a file

client = boto3.client('s3')
#obj = s3.get_object(Bucket='bucket', Key='key')
#df = pd.read_csv(io.BytesIO(obj['Body'].read()))

# print(f"~Mapper file {myfile} is read successfully from {mydir}.")
# Create a dataframe and write a file
my_data = {"coutry": ["a", "b"], "value": [1, 2]}
df = pd.DataFrame(my_data)

print (df.head())

df.to_json("snapshot.json")
df.to_csv('dataframe.csv')

# Create a json file and save it

# Read a json file