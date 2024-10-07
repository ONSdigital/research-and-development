"""
All platform-specific functions for the s3 file system that use boto3 and
raz_client.

These functions will need to be tested separately, using mocking.

Contains the following functions:
    create_client: Creates a boto3 client and sets raz_client argunents.
    rd_read_csv: Reads a CSV file from s3 to Pandas dataframe.
    rd_write_csv: Writes a Pandas Dataframe to csv in s3 bucket.
    rd_load_json: Loads a JSON file from s3 bucket to a Python dictionary.
    rd_file_exists: Checks if file exists in s3 using rdsa_utils.
    rd_mkdir(path: str): Creates a directory in s3 using rdsa_utils.

To do:
    Read  feather - possibly, not needed
    Write to feather - possibly, not needed
"""

# Standard libraries
import json
import logging


# Third party libraries
import pandas as pd
from io import StringIO, TextIOWrapper


# Local libraries
from rdsa_utils.cdp.helpers.s3_utils import (
    file_exists,
    create_folder_on_s3,
    delete_file,
    is_s3_directory,
    copy_file,
    move_file,
)
from src.utils.singleton_boto import SingletonBoto
# from src.utils.singleton_config import SingletonConfig

# set up logging, boto3 client and s3 bucket
s3_logger = logging.getLogger(__name__)
s3_client = SingletonBoto.get_client()
s3_bucket = SingletonBoto.get_bucket()

# Read a CSV file into a Pandas dataframe
def rd_read_csv(filepath: str, **kwargs) -> pd.DataFrame:
    """Reads a csv from s3 bucket into a Pandas Dataframe using boto3.
    If "thousands" argument is not specified, sets thousands=",", so that long
    integers with commas between thousands and millions, etc., are read
    correctly.
    Allows to use any additional keyword arguments of Pandas read_csv method.

    Args:
        filepath (str): Filepath (Specified in config)
        kwargs: Optional dictionary of Pandas read_csv arguments
    Returns:
        pd.DataFrame: Dataframe created from csv
    """

    with s3_client.get_object(Bucket=s3_bucket, Key=filepath)["Body"] as file:
        # If "thousands" argument is not specified, set it to ","
        if "thousands" not in kwargs:
            kwargs["thousands"] = ","

        # Read the csv file using the path and keyword arguments
        try:
            df = pd.read_csv(file, **kwargs)
        except Exception as e:
            s3_logger.error(f"Could not read specified file {filepath}. Error: {e}")

            raise e
    return df


def rd_write_csv(filepath: str, data: pd.DataFrame) -> None:
    """Write a Pandas Dataframe to csv in an s3 bucket.

    Args:
        filepath (str): The filepath to save the dataframe to.
        data (pd.DataFrame): THe dataframe to write to the passed path.

    Returns:
        None
    """
    # Create an Input-Output buffer
    csv_buffer = StringIO()

    # Write the dataframe to the buffer in the CSV format
    data.to_csv(
        csv_buffer, header=True, date_format="%Y-%m-%d %H:%M:%S.%f+00", index=False
    )

    # "Rewind" the stream to the start of the buffer
    csv_buffer.seek(0)

    # Write the buffer into the s3 bucket
    _ = s3_client.put_object(
        Bucket=s3_bucket, Body=csv_buffer.getvalue(), Key=filepath
    )
    return None


def rd_load_json(filepath: str) -> dict:
    """Load JSON data from an s3 bucket using a boto3 client.

    Args:
        filepath (string): The filepath in Hue s3 bucket.

    Returns:
        datadict (dict): The entire contents of the JSON file.
    """

    # Load the json file using the client method
    with s3_client.get_object(Bucket=s3_bucket, Key=filepath)["Body"] as json_file:
        datadict = json.load(json_file)

    return datadict


def rd_file_exists(filepath: str, raise_error=False) -> bool:
    """Function to check file exists in s3.

    Args:
        filepath (str): The filepath in s3.
        raise_error (bool): A switch to raise FileExistsError or not.

    Raises:
        FileExistsError: Raised if no file exists at the given filepath.

    Returns:
        result (bool): A boolean value which is true if the file exists.
    """

    result = file_exists(
        client=s3_client, bucket_name=s3_bucket, object_name=filepath
    )

    if not result and raise_error:
        raise FileExistsError(f"File: {filepath} does not exist")

    return result


def rd_mkdir(path: str) -> None:
    """Function to create a directory in s3 bucket.

    Args:
        path (str): The directory path to create

    Returns:
        None
    """

    _ = create_folder_on_s3(
        # client=config["client"],
        s3_client,
        bucket_name=s3_bucket,
        folder_path=path,
    )

    return None


def rd_write_feather(filepath, df):
    """Placeholder Function to write dataframe as feather file in HDFS"""
    return None


def rd_read_feather(filepath):
    """Placeholder Function to read feather file from HDFS"""
    return None

def rd_file_size(filepath: str) -> int:
    """Function to check the size of a file on s3 bucket.

    Args:
        filepath (string) -- The filepath in s3 bucket

    Returns:
        Int - an integer value indicating the size
        of the file in bytes
    """

    _response = s3_client.head_object(Bucket=s3_bucket, Key=filepath)
    file_size = _response['ContentLength']

    return file_size

def rd_delete_file(filepath: str) -> bool:
    """
    Delete a file from s3 bucket.
    Args:
        filepath (string): The filepath in s3 bucket to be deleted
    Returns:
        status (bool): True for successfully completed deletion. Else False.
    """
    status = delete_file(s3_client, s3_bucket, filepath)
    return status

def rd_md5sum(filepath: str) -> str:
    """
    Get md5sum of a specific file on s3.
    Args:
        filepath (string): The filepath in s3 bucket.
    Returns:
        md5result (int): The control sum md5.
    """

    try:
        md5result = s3_client.head_object(
            Bucket=s3_bucket,
            Key=filepath
        )['ETag'][1:-1]
    except s3_client.exceptions.ClientError as e:
        s3_logger.error(f"Failed to compute the md5 checksum: {str(e)}")
        md5result = None
    return md5result

def rd_isdir(dirpath: str): # -> bool:
    """
    Test if directory exists in s3 bucket.
    
    Args:
        dirpath (string): The "directory" path in s3 bucket.
    Returns:
        status (bool): True if the dirpath is a directory, false otherwise.

    """
    # The directory name must end with forward slash
    if not dirpath.endswith('/'):
        path = dirpath + '/' 
        
    # Any slashes at the beginning should be removed
    while dirpath.startswith('/'):
        dirpath = dirpath[1:]

    # Use the function from rdsa_utils
    response = is_s3_directory(client=s3_client, bucket_name=s3_bucket, object_name=dirpath)
    return response

def rd_isfile(filepath: str): # -> bool:
    """
    Test if given path is a file in s3 bucket. Check that it exists, not a directory and 
    the size is greater than 0.
    
    Args:
        filepath (string): The "directory" path in s3 bucket.
    Returns:
        status (bool): True if the dirpath is a directory, false otherwise.
    
    """
    if filepath is None:
        response = False
    
    if rd_file_exists(filepath):
        isdir = rd_isdir(filepath)
        size = rd_file_size(filepath)
        response = (not isdir) and (size > 0)
    else:
        response = False
    return response
    
def rd_stat_size(path: str) -> int:
    """
    Gets the file size of a file or directory in bytes.
    Alias of as rd_file_size.
    Works for directories, but returns 0 bytes, which is typical for s3.
    """
    return rd_file_size(path)

def rd_read_header(path: str) -> str:
    """
    Reads the first line of a file on s3. Gets the entire file using boto3 get_objects,
    converts its body into an input stream, reads the first line and remove the carriage
    return character (backslash-n) from the end.
        
    Args:
        filepath (string): The "directory" path in s3 bucket.
    
    Returns:
        status (bool): True if the dirpath is a directory, false otherwise.
    """
    # Create an input/output stream pointer, same as open
    stream = TextIOWrapper(s3_client.get_object(Bucket=s3_bucket, Key=path)['Body'])
    
    # Read the first line from the stream
    response = stream.readline()
    
    # Remove the last character (carriage return, or new line)
    response = response[:-1]
    
    return response
    
def rd_write_string_to_file(content: bytes, filepath: str):
    """
    Writes a string into the specified file path
    """
    
    # Put context to a new Input-Output buffer 
    str_buffer = StringIO(content.decode("utf-8"))

    # "Rewind" the stream to the start of the buffer
    str_buffer.seek(0)

    # Write the buffer into the s3 bucket
    _ = s3_client.put_object(
        Bucket=s3_bucket, Body=str_buffer.getvalue(), Key=filepath
    )
    return None

def _path_long2short(path: "str") -> str:
    """
    Extracts a short file name from the full path. 
    If there is at least one forward slash, finds the lates slash to the right
    and rerurns all characrers afrer it.
    
    If there are no slashes, returns the path as is.
    """
    if "/" in path:
        last_slash = path.rfind("/")
        return path[last_slash + 1:]
    else:
        return path

def _remove_end_slashes(path: "str") -> str:
    """
    Removes any amount of consequitive forward slashes from a path.
    """
    while path.endswith("/"):
        path = path[:-1]
        
    return path

def rd_copy_file(src_path: str, dst_path: str) -> bool:
    
    dst_path = _remove_end_slashes(dst_path)
    dst_path += "/" + _path_long2short(src_path)
    """
    Copy a file from one location to another. Uses rdsa_utils.
    """
    success = copy_file(
        client=s3_client,
        source_bucket_name=s3_bucket,
        source_object_name=src_path,
        destination_bucket_name=s3_bucket,
        destination_object_name=dst_path,
    ) 
    return success

def rd_move_file(src_path: str, dst_path: str) -> bool:
    """
    Move a file from one location to another. Uses rdsa_utils.
    
    """
    dst_path = _remove_end_slashes(dst_path)
    dst_path += "/" + _path_long2short(src_path)
    success = move_file(
        client=s3_client,
        source_bucket_name=s3_bucket,
        source_object_name=src_path,
        destination_bucket_name=s3_bucket,
        destination_object_name=dst_path
    )
    return success

# Function to replicate os.walk behavior
def s3walk(locations: list, prefix: str) -> tuple:
    """
    Mimics the functionality of os.walk in s3 bucket using long filenames with slashes.
    Recursively goes through the long filenames and splits it into "locations" - 
    subdirectories, and "files" - short file names.

    Args:
        locations (list): a list of s3 locations that can be "directories"
        prefix (str): Name of "subdirectory" of root where further locations will be found.
    
    Returns:
        A tuple of (root, (subdir, files)).
    """
    # recursively add location to roots starting from prefix
    def processLocation( root, prefixLocal, location):
        # add new root location if not available
        if not prefixLocal in root:
            root[prefixLocal]=(set(),set())
        # check how many folders are available after prefix
        remainder = location[len(prefixLocal):]
        structure = remainder.split('/')
        #if we are not yet in the folder of the file we need to continue with a larger prefix
        if len(structure)>1:
            # add folder dir
            root[prefixLocal][0].add(structure[0])
            #make sure file is added allong the way
            processLocation(root, prefixLocal+'/'+structure[0],location )
        else:
            # add to file
            root[prefixLocal][1].add(structure[0])

    root={}
    for location in locations:
        processLocation(root,prefix,location)

    return root.items()



def rd_search_file(dir_path: str, ending: str) -> str:
    """Find a file in a directory with a specific ending.
    
    Args:
        dir_path (str): s3 "directory" where to search for files
        ending (str): File name ending to search for.
    Returns:
        Full file name that ends with the given string.
    
    """
    target_file = None
    
    # Remove preceding forward slashes if needed
    while dir_path.startswith("/"):
        dir_path = dir_path[1:]
    
    # get list of objects with prefix
    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=dir_path)

    # retrieve key values
    locations = [object['Key'] for object in response['Contents']]
       
    for _, (__, files) in s3walk(locations, dir_path):
        for file in files:

            # Check for ending
            if file.endswith(ending):
                target_file = str(file)
    return target_file

    
    















