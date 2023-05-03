import pydoop.hdfs as hdfs
import pandas as pd
import json

from src.utils.helpers import Config_settings


conf_obj = Config_settings()
config = conf_obj.config_dict
snapshot_path = config["snapshot_path"]  # Taken from config file


def hdfs_load_json(filepath: str):
    """Function to load JSON data from DAP
    Args:
        filepath (string): The filepath in Hue
    """

    # Open the file in read mode inside Hadoop context
    with hdfs.open(filepath, "r") as file:
        # Import csv file and convert to Dataframe
        datadict = json.load(file)

    return datadict


snapdata = hdfs_load_json(snapshot_path)

contributerdict = snapdata["contributors"][0]
responsesdict = snapdata["responses"][0]


def dict_to_pd(dict: dict) -> pd.DataFrame:
    """Converts Dict object to Pandas Dataframe

    Args:
        dict (dict): Dict to be converted

    Returns:
        pd.DataFrame: Dataframe with correct orientation
    """
    datadf = pd.DataFrame.from_dict(dict, orient="index")

    return datadf


datadf = dict_to_pd(snapdata)

print(contributerdict)
print("\n")
print(responsesdict)
print("\n")
print(datadf[0][1][0])
