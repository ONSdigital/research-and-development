import pandas as pd

from src.utils.helpers import Config_settings
from src.utils.hdfs_mods import hdfs_load_json

conf_obj = Config_settings()
config = conf_obj.config_dict
snapshot_path = config["snapshot_path"]  # Taken from config file

snapdata = hdfs_load_json(snapshot_path)

contributerdict = snapdata["contributors"]
responsesdict = snapdata["responses"]

datadf = pd.DataFrame.from_dict(snapdata, orient="index")

print(contributerdict[0])
print("\n")
print(responsesdict[0])
