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

# Third party libraries specific to s3 bucket
import boto3
import raz_client

# Local libraries
from rdsa_utils.cdp.helpers.s3_utils import file_exists, create_folder_on_s3

# set up logging
s3_logger = logging.getLogger(__name__)


def create_client(config: dict):
    """Initialises and configures a boto3 client. Configures the raz_client,
    which is needed for authentication between CDSW session and the s3 server,
    using the parameters stored in the config.

    Args:
        config (dict): Combined config (s3 parameters are in developer config)
    Returns:
        boto3 client
    """
    client = boto3.client("s3")
    raz_client.configure_ranger_raz(client, ssl_file=config["s3"]["ssl_file"])
    return client


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
    # Open the boto3 client
    s3_client = config["client"]
    with s3_client.get_object(
        Bucket=config["s3"]["s3_bucket"],
        Key=filepath
    )['Body'] as file:

        # If "thousands" argument is not specified, set it to ","
        if "thousands" not in kwargs:
            kwargs["thousands"] = ","

        # Read the scv file using the path and keyword arguments
        try:
            df = pd.read_csv(file, **kwargs)
        except Exception:
            if "usecols" in kwargs:
                s3_logger.info("Columns not found: " + str(kwargs["usecols"]))
            if kwargs:
                s3_logger.info("The following arguments failed: " + str(kwargs))

            s3_logger.error(f"Could not read specified file: {filepath}")

            raise ValueError
    return df


def rd_write_csv(filepath: str, data: pd.DataFrame) -> None:
    """Writes a Pandas Dataframe to csv in s3 bucket

    Args:
        filepath (str): Filepath (Specified in config)
        data (pd.DataFrame): Data to be stored
    Returns:
        None
    """
    # Create an Unput-Output buffer
    csv_buffer = StringIO()

    # Write the dataframe to the buffer in the CSV format
    data.to_csv(
        csv_buffer,
        header=True,
        date_format="%Y-%m-%d %H:%M:%S.%f+00",
        index=False
    )

    # "Rewind" the stream to the start of the buffer
    csv_buffer.seek(0)

    # Use the boto3 client from the config
    s3_client = config["client"]

    # Write the buffer into the s3 bucket
    _ = s3_client.put_object(
        Bucket=config["s3"]["s3_bucket"],
        Body=csv_buffer.getvalue(),
        Key=filepath
    )
    return None


def rd_load_json(filepath: str) -> dict:
    """Function to load JSON data from s3 bucket using a boto3 client
    Args:
        filepath (string): The filepath in Hue s3 bucket
    Returns:
        datadict (dict): The entire content of the JSON file
    """
    # Use the boto3 client from the config
    s3_client = config["client"]

    # Load the json file using the client method
    with s3_client.get_object(
        Bucket=config["s3"]["s3_bucket"],
        Key=filepath
    )['Body'] as json_file:
        datadict = json.load(json_file)

    return datadict


def rd_file_exists(filepath: str, raise_error=False) -> bool:
    """Function to check file exists in s3.

        Args:
            filepath (str): The filepath in s3
            raise_error (bool): A switch to raise FileExistsError or not.

        Returns:
            result (bool): A boolean value which is true if the file exists.
    """

    result = file_exists(
        client=config["client"],
        bucket_name=config["s3"]["s3_bucket"],
        object_name=filepath)

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
        client=config["client"],
        bucket_name=config["s3"]["s3_bucket"],
        folder_path=path,
    )

    return None


def rd_write_feather(filepath, df):
    """Placeholder Function to write dataframe as feather file in HDFS"""
    return None


def rd_read_feather(filepath):
    """Placeholder Function to read feather file from HDFS"""
    return None