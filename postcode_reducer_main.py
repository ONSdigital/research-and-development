"""Main pipeline file for the exporting of files"""
import os

from importlib import reload
from src.utils import postcode_reduction_helper

reload(postcode_reduction_helper)

user_path = os.path.join("src", "user_config.yaml")
dev_path = os.path.join("src", "dev_config.yaml")

if __name__ == "__main__":
    postcode_reduction_helper.run_postcode_reduction(user_path, dev_path)
