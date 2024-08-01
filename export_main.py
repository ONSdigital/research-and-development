"""Main pipeline file for the exporting of files"""
import os

from importlib import reload
from src.outputs import export_files

reload(export_files)

user_path = os.path.join("src", "user_config.yaml")
dev_path = os.path.join("src", "dev_config.yaml")

if __name__ == "__main__":
    export_files.run_export(user_path, dev_path)
