import pandas as pd

from src.utils.helpers import Config_settings
from src.utils.hdfs_mods import hdfs_load_json

conf_obj = Config_settings()
config = conf_obj.config_dict
snapshot_path = config["paths"]["snapshot_path"]  # Taken from config file


def load_snapshot_data(snapshot_path, data_type):

    """Load data from SPP Snapshot file in HUE and return two DataFrames containing
    contributor and response data respectively.

    Arguments:
        snapshot_path -- Filepath
        data_type -- String with value either "contributors" or "responses".
                     Determines which part of the snapshot file should be loaded.

    Returns:
        data -- DataFrame containing either contributor or response data for BERD
                from SPP Snapshot file
    """

    snapshot_data = hdfs_load_json(snapshot_path)

    data = pd.DataFrame(snapshot_data[data_type])

    return data