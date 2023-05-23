import pandas as pd
from typing import Tuple

from src.utils.helpers import Config_settings
from src.utils.hdfs_mods import hdfs_load_json


conf_obj = Config_settings()
config = conf_obj.config_dict
snapshot_path = config["paths"]["snapshot_path"]  # Taken from config file

def parse_snap_data(snapdata: dict = snapshot_path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Loads the data from the survey via the SPP snapshot. The data is supplied as dict
        and is parsed into dataframes, one for survey contributers (company details)
        and another one for their responses.

    Args:
        snapdata (dict, optional): The data from the SPP snapshot. Defaults to snapdata.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: The contributers and responders dataframes
    """
    # Load the dicts
    snapdata = hdfs_load_json(snapshot_path)
    contributordict = snapdata["contributors"]
    responsesdict = snapdata["responses"]

    # Make dataframes
    contributors_df = pd.DataFrame(contributordict)
    responses_df = pd.DataFrame(responsesdict)

    return contributors_df, responses_df