import pandas as pd
from src.utils.helpers import Config_settings
import numpy as np

import logging

pg_logger = logging.getLogger(__name__)

conf_obj = Config_settings()
config = conf_obj.config_dict

# Check the environment switch
network_or_hdfs = config["global"]["network_or_hdfs"]

if network_or_hdfs == "network":
    HDFS_AVAILABLE = False
    from src.utils.local_file_mods import read_local_mapper_csv as read_csv

elif network_or_hdfs == "hdfs":
    HDFS_AVAILABLE = True
    from src.utils.hdfs_mods import read_hdfs_mapper_csv as read_csv

else:
    pg_logger.error("The network_or_hdfs configuration is wrong")
    raise ImportError


def pg_mapper(
    df: pd.DataFrame,
    target_col: str,
    from_col: str = "2016 > Form PG",
    to_col: str = "2016 > Pub PG",
):
    mapper_path = config[f"{network_or_hdfs}_paths"]["mapper_path"]
    mapper = read_csv(mapper_path, from_col, to_col).squeeze()
    map_dict = dict(zip(mapper[from_col], mapper[to_col]))
    map_dict = {i: j for i, j in map_dict.items()}

    df[target_col] = pd.to_numeric(df[target_col], errors="coerce")
    df[target_col] = df[target_col].replace({0: np.nan})
    df.replace({target_col: map_dict}, inplace=True)
    df[target_col] = df[target_col].astype("category")

    pg_logger.info("Product groups successfully mapped to letters")
    return df
