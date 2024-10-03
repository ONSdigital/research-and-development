"""Script that creates all directories"""
import os

# Change to the project repository location
my_wd = os.getcwd()
my_repo = "research-and-development"
if not my_wd.endswith(my_repo):
    os.chdir(my_repo)

from src.utils.helpers import tree_to_list
import src.utils.s3_mods as mods
from src.utils.singleton_boto import SingletonBoto

config = {
    "s3": {
        "ssl_file": "/etc/pki/tls/certs/ca-bundle.crt",
        "s3_bucket": "onscdp-dev-data01-5320d6ca"
    }
}

boto3_client = SingletonBoto.get_client(config)


def run_file_size():
    root = "/bat/res_dev/project_data"


if __name__ == "__main__":
    run_make_dirs()
