"""Script that creates all directories"""
import os

# Change to the project repository location
my_wd = os.getcwd()
my_repo = "research-and-development"
if not my_wd.endswith(my_repo):
    os.chdir(my_repo)

from src.utils.helpers import tree_to_list
from src.utils.singleton_boto import SingletonBoto

config = {
    "s3": {
        "ssl_file": "/etc/pki/tls/certs/ca-bundle.crt",
        "s3_bucket": "onscdp-dev-data01-5320d6ca"
    }
}

boto3_client = SingletonBoto.get_client(config)
import src.utils.s3_mods as mods




if __name__ == "__main__":
    
    my_path = "/bat/res_dev/project_data/2023_surveys/BERD/01_staging/staging_qa/full_responses_qa/2023_staged_BERD_full_responses_24-10-02_v17.csv"
    my_size = mods.rd_file_size(my_path)
    
    print(f"File size is {my_size}")
    
    status = mods.rd_delete_file(my_path)
    if status:
        print(f"File {my_path} successfully deleted")
    
