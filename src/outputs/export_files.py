"""This is a stand alone pipeline to selectively transfer output files from
the output folder to the outgoing folder, along with their manifest file."""


import os
import logging
from datetime import datetime
import toml

from src.utils.helpers import Config_settings
from src.outputs.manifest_output import Manifest


# Set up logging
OutgoingLogger = logging.getLogger(__name__)


config_path = os.path.join("src", "developer_config.yaml")

# load config
conf_obj = Config_settings(config_path)
config = conf_obj.config_dict

# Check the environment switch
network_or_hdfs = config["global"]["network_or_hdfs"]

if network_or_hdfs == "network":

    from src.utils.local_file_mods import local_list_files as list_files
    from src.utils.local_file_mods import local_copy_file as copy_files
    from src.utils.local_file_mods import local_move_file as move_files
    from src.utils.local_file_mods import local_search_file as search_files
    from src.utils.local_file_mods import local_isfile as isfile
    from src.utils.local_file_mods import local_delete_file as delete_file
    from src.utils.local_file_mods import local_md5sum as hdfs_md5sum
    from src.utils.local_file_mods import local_stat_size as hdfs_stat_size
    from src.utils.local_file_mods import local_isdir as isdir
    from src.utils.local_file_mods import local_read_header as read_header
    from src.utils.local_file_mods import (
        local_write_string_to_file as write_string_to_file,
    )

elif network_or_hdfs == "hdfs":

    from src.utils.local_file_mods import hdfs_list_files as list_files
    from src.utils.hdfs_mods import hdfs_copy_file as copy_files
    from src.utils.hdfs_mods import hdfs_move_file as move_files
    from src.utils.hdfs_mods import hdfs_search_file as search_files
    from src.utils.hdfs_mods import hdfs_isfile as isfile
    from src.utils.hdfs_mods import hdfs_delete_file as delete_file
    from src.utils.hdfs_mods import hdfs_md5sum as hdfs_md5sum
    from src.utils.hdfs_mods import hdfs_stat_size as hdfs_stat_size
    from src.utils.hdfs_mods import hdfs_isdir as isdir
    from src.utils.hdfs_mods import hdfs_read_header as read_header
    from src.utils.hdfs_mods import hdfs_write_string_to_file as write_string_to_file


else:
    OutgoingLogger.error("The network_or_hdfs configuration is wrong")
    raise ImportError

OutgoingLogger.info(f"Using the {network_or_hdfs} file system as data source.")


# Define paths
NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
paths = config[f"{NETWORK_OR_HDFS}_paths"]
output_path = paths["output_path"]
short_form_output = os.path.join(output_path, "output_short_form")
outgoing_folder = paths["outgoing_path"]

# Getting correct headers to verify that df and output are the same
s_f_schema_path = paths["short_form_schema"]
short_form_schema = toml.load(s_f_schema_path)
short_form_headers = short_form_schema["headers"]
schema_columns_str = ",".join(short_form_headers)

# Create a datetime object for the pipeline run - TODO: replace this with
# the pipeline run datetime from the runlog object
pipeline_run_datetime = datetime.now()


def get_file_choice(output_path):
    """Prompt user to select a files to transfer.

    Returns:
        selction_list (list): A list of the files to transfer."""
    file_list = list_files(output_path)
    choices = []
    print("Select a file to transfer:")
    for i, file in enumerate(file_list):
        print(f"{i+1}. {file}")
    file_num = "X"  # initialize file_num to a non-integer value
    while True and file_num != "Q":
        try:
            file_num = input("Enter file number or press 'Q' to quit: ")
            file_num = int(file_num)
            choices.append(file_num - 1)
            if file_num < 1 or file_num > len(file_list):
                print("Invalid input. Please enter a valid file number.")
                continue
        except ValueError:
            print("Invalid input. Please enter a valid file number.")
    selction_list = [file_list[num] for num in choices]

    print(f"These are the file selected: \n {[file for file in selction_list]}")

    return selction_list


def check_files_exist(dir, file_list):
    """Check that all the files in the file list exist using
    the imported isfile function."""
    for file in file_list:
        file_path = os.path.join(dir, file)
        if not isfile(file_path):
            OutgoingLogger.error(f"File {file} does not exist.")
            raise FileNotFoundError


def run_export():
    """Main function to run the data export pipeline."""
    # Get list of files to transfer from user
    file_list = get_file_choice(short_form_output)

    # Check that files exist
    check_files_exist(short_form_output, file_list)

    # Creating a manifest object using the Manifest class in manifest_output.py
    manifest = Manifest(
        outgoing_directory=output_path,
        pipeline_run_datetime=pipeline_run_datetime,
        dry_run=True,
        delete_file_func=delete_file,
        md5sum_func=hdfs_md5sum,
        stat_size_func=hdfs_stat_size,
        isdir_func=isdir,
        isfile_func=isfile,
        read_header_func=read_header,
        string_to_file_func=write_string_to_file,
    )

    # Add the short form output file to the manifest object
    for filename in file_list:
        manifest.add_file(
            f"{output_path}/output_short_form/{filename}",
            column_header=schema_columns_str,
            validate_col_name_length=True,
            sep=",",
        )

    # Write the manifest file to the outgoing directory
    manifest.write_manifest()

    # Copy files to outgoing folder
    copy_files(file_list)

    # Move the manifest file to the outgoing folder
    manifest_file = search_files(output_path, "_manifest.json")
    move_files(manifest_file)

    # Log success message
    OutgoingLogger.info("Files transferred successfully.")


if __name__ == "__main__":
    run_export()
