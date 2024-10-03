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
    Copy file
    Move file
    Compute md5 sum
    TBC
"""

# Standard libraries
import json
import logging


# Third party libraries
import pandas as pd
from io import StringIO

# Local libraries
from rdsa_utils.cdp.helpers.s3_utils import (
    file_exists,
    create_folder_on_s3,
    delete_file,
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

def rd_md5sum(filepath: str) -> int:
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
    except ValueError:
        md5result = None
        pass
    return md5result





