# To test the ability to read, write and check if the file exists in s3 file system

# user parameters
mydir = "/user/george.zorinyants/res_dev/2022_surveys/mappers/v1"
myfile = "pg_num_alpha_2022.csv"

# load libraries
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

import boto3import raz_clientimport pandas as pdfrom rdsa_utils.cdp.helpers.s3_utils import *logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
# Load configuration; this could be from a file, environment, etc.config = {
    "ssl_file": "/etc/pki/tls/certs/ca-bundle.crt",
    "s3_bucket": "onscdp-dev-data01-5320d6ca",
}
file = "user/dominic.bean/iris.csv"################################################################################client = boto3.client("s3")raz_client.configure_ranger_raz(client, ssl_file=config["ssl_file"])
################################################################################list_files(client, config["s3_bucket"], "/user/dominic.bean/output_test")
 