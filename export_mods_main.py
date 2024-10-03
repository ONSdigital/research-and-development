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
    
    my_path = "/bat/res_dev/project_data/2023_surveys/BERD/01_staging/staging_qa/full_responses_qa/2023_staged_BERD_full_responses_24-10-02_v20.csv"

#     # Checking that file exists
#     my_size = mods.rd_file_size(my_path)
#     print(f"File size is {my_size}")
    
#     # Deleting a file
#     status = mods.rd_delete_file(my_path)
#     if status:
#         print(f"File {my_path} successfully deleted")
    
    # # Calculating md5sum
    # my_sum = mods.rd_md5sum(my_path)
    # expected_output = "ea94424aceecf11c8a70d289e51c34ea"
    # print(type(my_sum))
    # if expected_output == my_sum:
    #     print("Same md5sum")
    
#     # Calculating rd_isdir 
#     mydir = "bat"
#     response = mods.rd_isdir(mydir)
    
#     print("Got response")
#     print(response)
    
    # Checking rd_isfile
    response = mods.rd_isfile(my_path)
    print(response)
