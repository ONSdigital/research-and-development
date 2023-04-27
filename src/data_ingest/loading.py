import pydoop.hdfs as hdfs
import pandas as pd
import json

# from src.utils.helpers import Config_settings


# conf_obj = Config_settings()
# config = conf_obj.config_dict
# context = os.getenv("HADOOP_USER_NAME")  # Put your context name here
# project = config["logs_foldername"]  # Taken from config file
# file_name = "Loadin"
# main_path = f"/user/{context}/{project}"
# hdfs.mkdir(main_path)
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

print(contributerdict, responsesdict)
