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

contributerdict = snapdata["contributors"]
responsesdict = snapdata["responses"]

contributers = pd.DataFrame(contributerdict)
responses = pd.DataFrame(responsesdict)

print(contributers.head())
print("\n")
print(responses.head())
print("\n")
print([responses["questioncode"].unique()])
