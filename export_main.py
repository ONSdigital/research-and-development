"""Main pipeline file for the exporting of files"""

from importlib import reload
from src.outputs import export_files

reload(export_files)

config_path = r"src\developer_config.yaml"

if __name__ == "__main__":
    export_files.run_export(config_path)
