"""This is a stand alone pipeline to selectively transfer output files from the output folder to the outgoing folder, along with their manifest file."""


import os
import shutil
import logging

from src.utils.helpers import Config_settings

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

elif network_or_hdfs == "hdfs":

    from src.utils.local_file_mods import hdfs_list_files as list_files
    from src.utils.hdfs_mods import hdfs_copy_file as copy_files


else:
    OutgoingLogger.error("The network_or_hdfs configuration is wrong")
    raise ImportError

OutgoingLogger.info(f"Using the {network_or_hdfs} file system as data source.")


# Define paths
output_folder = "/path/to/output/folder"
outgoing_folder = "/path/to/outgoing/folder"
manifest_file = "/path/to/manifest/file"


# import correct functions according to environment
if os.name == "nt":
    # Windows
    from src.utils.local_file_mods import check_files_exist
elif os.name == "posix":
    # Linux
    from src.utils.hdfs_mods import check_files_exist





def get_file_list():
    """Prompt user to select a file to transfer."""
    file_list = os.listdir(output_folder)
    print("Select a file to transfer:")
    for i, file in enumerate(file_list):
        print(f"{i+1}. {file}")
    while True:
        try:
            file_num = int(input("Enter file number: "))
            if file_num < 1 or file_num > len(file_list):
                raise ValueError
            break
        except ValueError:
            print("Invalid input. Please enter a valid file number.")
    file_name = file_list[file_num-1]
    return file_name



def create_manifest_file(file_list):
    """Create manifest file with list of files."""
    with open(manifest_file, "w") as f:
        f.write("\n".join(file_list))
    OutgoingLogger.info(f"Manifest file created: {manifest_file}")



def main():
    """Main function to run pipeline."""
    # Get list of files to transfer
    file_list = get_file_list()

    # Check that files exist
    check_files_exist(file_list)

    # Create manifest file
    create_manifest_file(file_list)

    # Copy files to outgoing folder
    copy_files(file_list)

    # Log success message
    logging.info("Files transferred successfully.")

if __name__ == "__main__":
    main()
