import pandas as pd
from src.utils.helpers import Config_settings
import numpy as np

conf_obj = Config_settings()
config = conf_obj.config_dict

# Check the environment switch
network_or_hdfs = config["global"]["network_or_hdfs"]

# Conditional Import
try:
    import pydoop.hdfs as hdfs

    HDFS_AVAILABLE = True
except ImportError:
    HDFS_AVAILABLE = False


def pg_mapper(
    df: pd.DataFrame,
    mapper_path: str,
    target_col: str,
    from_col: str = "2016 > Form PG",
    to_col: str = "2016 > Pub PG",
):
    # PG mapping
    if network_or_hdfs == "network":
        # Load SIC to PG mapper
        mapper = pd.read_csv(mapper_path, usecols=[from_col, to_col]).squeeze()

    elif network_or_hdfs == "hdfs":
        with hdfs.open(mapper_path, "r") as file:
            # Load SIC to PG mapper
            mapper = pd.read_csv(file, usecols=[from_col, to_col]).squeeze()
    else:
        raise ImportError

    map_dict = dict(zip(mapper[from_col], mapper[to_col]))
    map_dict = {i: j for i, j in map_dict.items()}

    df[target_col] = pd.to_numeric(df[target_col], errors="coerce")
    df[target_col] = df[target_col].replace({0: np.nan})
    df.replace({target_col: map_dict}, inplace=True)
    df[target_col] = df[target_col].astype("category")

    return df
