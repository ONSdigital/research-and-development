import pandas as pd

from src.utils.helpers import Config_settings
from src.utils.hdfs_mods import hdfs_load_json

conf_obj = Config_settings()
config = conf_obj.config_dict
snapshot_path = config["paths"]["snapshot_path"]  # Taken from config file

snapdata = hdfs_load_json(snapshot_path)

contributerdict = snapdata["contributors"]
responsesdict = snapdata["responses"]

contributers = pd.DataFrame(contributerdict)
responses = pd.DataFrame(responsesdict)

print(contributers.head())
print("\n")
print(responses.head())
print("\n")
print([responses["questioncode"].unique()])
import pydoop.hdfs as hdfs
import pandas as pd
import json

file_path = (
    "/ons/rdbe_dev/snapshot-202012-002-fba5c4ba-fb8c-4a62-87bb-66c725eea5fd.json"
)


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


snapdata, contributerdict, responsesdict = hdfs_load_json(file_path)
