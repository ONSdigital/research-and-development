# General functions
import json
import pandas as pd
from io import StringIO
import logging
from typing import List
import subprocess
import os

# Functions specific to s3 bucket
import boto3
import raz_client
from rdsa_utils.cdp.helpers.s3_utils import file_exists, create_folder_on_s3


# set up logging
s3_logger = logging.getLogger(__name__)


def create_client(config):
    client = boto3.client("s3")
    raz_client.configure_ranger_raz(client, ssl_file=config["s3"]["ssl_file"])
    return client


# Read a CSV file into a Pandas dataframe
def rd_read_csv(filepath: str, **kwargs) -> pd.DataFrame:
    """Reads a csv from s3 bucket into a Pandas Dataframe using pydoop. 
    If "thousands" argument is not specified, sets it to ",". 
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
                rd_logger.info("Columns not found: " + str(kwargs["usecols"]))
            rd_logger.error(f"Could not read specified file: {filepath}")

            raise ValueError
    return df
  

def rd_write_csv(filepath: str, data: pd.DataFrame) -> None:
    """Writes a Pandas Dataframe to csv in s3 bucket

    Args:
        filepath (str): Filepath (Specified in config)
        data (pd.DataFrame): Data to be stored
    """
    # Create an Unput-Output buffer
    csv_buffer = StringIO()
    
    # Write the dataframe to the buffer in the CSV format 
    df.to_csv(csv_buffer, header=True, date_format="%Y-%m-%d %H:%M:%S.%f+00", index=False)
    
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
    """Function to load JSON data from s3 bucket
    Args:
        filepath (string): The filepath in Hue
    """
    # Use the boto3 client from the config
    s3_client = config["client"]
    
    # Load the json file using the client method
    with s3_client.get_object(Bucket=config["s3"]["s3_bucket"], Key=filepath)['Body'] as json_file:
        datadict = json.load(json_file)
    
    return datadict


def rd_file_exists(filepath: str, raise_error=False) -> bool:
    """Function to check file exists in hdfs.

        Args:
            filepath (string) -- The filepath in s3

        Returns:
            Bool - A boolean value which is true if file exists
    """

    result = file_exists(
        client=config["client"],
        bucket_name=config["s3"]["s3_bucket"],
        object_name=filepath)

    if not result and raise_error:
        raise FileExistsError(f"File: {filepath} does not exist")

    return result



def rd_mkdir(path: str):
    """Function to create a directory in s3 bucket

    Args:
        path (string) -- The path to create
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
