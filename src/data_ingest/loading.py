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
        contributerdict = datadict["contributors"][0]
        responsesdict = datadict["responses"][0]

    datadf = pd.DataFrame.from_dict(datadict, orient="index")

    return datadf, contributerdict, responsesdict


snapdata, contributerdict, responsesdict = hdfs_load_json(snapshot_path)

print(contributerdict)
print("\n")
print(responsesdict)
