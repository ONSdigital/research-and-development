"""This is a stand alone pipeline to selectively transfer output files from
the output folder to the outgoing folder, along with their manifest file."""


import os
import logging
from datetime import datetime
import toml
from typing import List
from pathlib import Path

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
    from src.utils.local_file_mods import local_copy_file as move_files
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

    from src.utils.hdfs_mods import hdfs_list_files as list_files
    from src.utils.hdfs_mods import hdfs_copy_file as move_files
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
paths = config[f"{network_or_hdfs}_paths"]
output_path = paths["output_path"]
short_form_output = os.path.join(output_path, "output_short_form")
export_folder = paths["export_path"]

# Getting correct headers to verify that df and output are the same
s_f_schema_path = config["schema_paths"]["frozen_shortform_schema"]
short_form_schema = toml.load(s_f_schema_path)

def get_schema_headers(file_path_dict: dict, config: dict = config):
    
    schema_paths = config['schema_paths']
    
    schema_paths = {output_name[7:]: schema_paths[f"{output_name[7:]}_schema"] 
                    for output_name in file_path_dict.keys()]
    
    ### *** GOT HERE WIP
    
    # Get the headers from the short form schema
    short_form_headers = short_form_schema.keys()
    schema_columns_str = ",".join(short_form_headers)


# Create a datetime object for the pipeline run - TODO: replace this with
# the pipeline run datetime from the runlog object
pipeline_run_datetime = datetime.now()


def get_file_choice(config: dict = config):
    """Get files to transfer from the 'export_choices' section of the config.

    Returns:
        selection_list (list): A list of the files to transfer."""
        
    # Get the user's choices from config
    export_choices = config.get('export_choices', {})
    
    paths = config[f"{config['global']['network_or_hdfs']}_paths"]
    output_path = paths['output_path']
        
    # Use list comprehension to create the selection list
    selection_list = [Path(f"{output_path}/{dir}/{file}").with_suffix('.csv') 
                      for dir, file in export_choices.items() 
                      if file is not None]
    
    selection_dict = {dir[7:]:file_path for dir,file_path
                    in zip(export_choices.keys(), selection_list)}

    # Log the files being exported
    logging.info(f"These are the files being exported: {selection_list}")

    return selection_dict


def check_files_exist(output_dirs: str, file_list: List):
    """Check that all the files in the file list exist using
    the imported isfile function."""
    
    # Check if the output dirs supplied are string, change to list if so
    if isinstance(output_dirs, str):
        output_dirs = [output_dirs]
    
    # Get all files listed in all the output directories
    all_files = []
    for dir in output_dirs:
        all_files.extend(list_files(dir))

    for file in file_list:
        file_path = Path(file)
        if not file_path.is_file():
            OutgoingLogger.error(f"File {file} does not exist in {all_files}.")
            raise FileNotFoundError


def run_export():
    """Main function to run the data export pipeline."""
    # Get list of files to transfer from user
    file_select_dict = get_file_choice()

    # Check that files exist
    check_files_exist(short_form_output, list(file_select_dict.values))

    # Creating a manifest object using the Manifest class in manifest_output.py
    manifest = Manifest(
        outgoing_directory=output_path,
        export_directory=export_folder,
        pipeline_run_datetime=pipeline_run_datetime,
        dry_run=False,
        delete_file_func=delete_file,
        md5sum_func=hdfs_md5sum,
        stat_size_func=hdfs_stat_size,
        isdir_func=isdir,
        isfile_func=isfile,
        read_header_func=read_header,
        string_to_file_func=write_string_to_file,
    )
    
    schemas_header = get_schema_headers(file_select_dict)

    # Add the short form output file to the manifest object
    for file_path in file_select_dict:
        manifest.add_file(
            file_path,
            column_header=schema_columns_str,
            validate_col_name_length=True,
            sep=",",
        )

    # Write the manifest file to the outgoing directory
    manifest.write_manifest()

    # Move the manifest file to the outgoing folder
    manifest_file = search_files(manifest.outgoing_directory, "_manifest.json")

    manifest_path = os.path.join(manifest.outgoing_directory, manifest_file)
    move_files(manifest_path, manifest.export_directory)

    # Copy files to outgoing folder
    for file_path in file_select_dict:
        file_path = os.path.join(file_path)
        move_files(file_path, manifest.export_directory)

    # Log success message
    OutgoingLogger.info("Files transferred successfully.")


if __name__ == "__main__":
    run_export()
