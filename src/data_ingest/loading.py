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

    # Open the file in read mode inside Hadoop context
    with hdfs.open(filepath, "r") as file:
        # Import csv file and convert to Dataframe
        datadict = json.load(file)
        contributerdict = datadict["contributors"][0]
        responsesdict = datadict["responses"][0]

    datadf = pd.DataFrame.from_dict(datadict, orient="index")

    return datadf, contributerdict, responsesdict


snapdata, contributerdict, responsesdict = hdfs_load_json(file_path)

# test commit
