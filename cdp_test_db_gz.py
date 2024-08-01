import boto3
import raz_client
import json
import pandas as pd
from io import StringIO

from rdsa_utils.cdp.helpers.s3_utils import *
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
# Load configuration; this could be from a file, environment, etc.
config = {
   "ssl_file": "/etc/pki/tls/certs/ca-bundle.crt",
   "s3_bucket": "onscdp-dev-data01-5320d6ca",
}
file = "user/dominic.bean/iris.csv"
################################################################################
client = boto3.client("s3")
raz_client.configure_ranger_raz(client, ssl_file=config["ssl_file"])
################################################################################
list_files(client, config["s3_bucket"], "/user/george.zorinyants")

################################################################################
# Read a CSV file into a Pandas dataframe
with client.get_object(Bucket=config["s3_bucket"], Key='user/george.zorinyants/pg_num_alpha_2023.csv')['Body'] as csv_f:
  rescue_df = pd.read_csv(csv_f)
  
rescue_df.head()

################################################################################
# Create a dataframe and write to a CSV file
my_data = {"coutry": ["a", "b"], "value": [1, 2]}
df = pd.DataFrame(my_data)
df.head()


csv_buffer = StringIO()
df.to_csv(csv_buffer, header=True, index=False)
csv_buffer.seek(0) # To position the stream at the start of the buffer

_ = client.put_object(
    Bucket=config["s3_bucket"], 
    Body=csv_buffer.getvalue(), 
    Key='user/george.zorinyants/test_output.csv'
)

###############################################################################
# Read a json file into a Python dictionary
with client.get_object(
    Bucket=config["s3_bucket"], 
    Key='user/george.zorinyants/snapshot.json'
)['Body'] as json_file:
  
    my_dict = json.load(json_file)
    
print(my_dict)