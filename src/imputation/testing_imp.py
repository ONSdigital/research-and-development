import pandas as pd
from src.utils.helpers import Config_settings

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

# Load the files from the correct location
if network_or_hdfs == "network":
    HDFS_AVAILABLE = False

    pre_path = "R:/BERD Results System Development 2023/DAP_emulation/BERD_V7_Anonymised/qv_BERD_202012_qv6_reformatted.csv"  # noqa
    cur_path = "R:/BERD Results System Development 2023/DAP_emulation/BERD_V7_Anonymised/qv_BERD_202112_qv6_reformatted.csv"  # noqa
    map_path = "R:/BERD Results System Development 2023/DAP_emulation/mappers/SIC07 to PG Conversion - From 2016 Data .csv"  # noqa

    pre_df = pd.read_csv(
        pre_path,
        na_values="                                                                ",
    )
    cur_df = pd.read_csv(
        cur_path,
        na_values="                                                                ",
    )
    # Load SIC to PG mapper
    mapper = pd.read_csv(
        map_path, usecols=["2016 > Form PG", "2016 > Pub PG"]
    ).squeeze()


elif network_or_hdfs == "hdfs":
    HDFS_AVAILABLE = False

    pre_path = "/ons/rdbe_dev/BERD_V7_Anonymised/qv_BERD_202012_qv6_reformatted.csv"
    cur_path = "/ons/rdbe_dev/BERD_V7_Anonymised/qv_BERD_202112_qv6_reformatted.csv"
    map_path = "/ons/rdbe_dev/BERD_Mappers/SIC07_to_PG_Conversion-From_2016_Data.csv"

    with hdfs.open(pre_path, "r") as file:
        # Import csv file and convert to Dataframe
        pre_df = pd.read_csv(
            file,
            na_values="                                                                ",  # noqa
        )

    with hdfs.open(cur_path, "r") as file:
        # Import csv file and convert to Dataframe
        cur_df = pd.read_csv(
            file,
            na_values="                                                                ",  # noqa
        )

    with hdfs.open(map_path, "r") as file:
        # Load SIC to PG mapper
        mapper = pd.read_csv(
            file, usecols=["2016 > Form PG", "2016 > Pub PG"]
        ).squeeze()

else:
    raise ImportError

# Data cleaning
pre_df.columns.str.replace(" ", "")

pre_df.loc[pre_df["returned_value"].isnull(), "returned_value"] = pre_df[
    "adjusted_value"
]
cur_df["returned_value"] = cur_df["instance"]
cur_df.loc[cur_df["returned_value"] == 0, "returned_value"] = cur_df["adjusted_value"]

cur_df.loc[(cur_df["returned_value"] == 1), "returned_value"] = 0
cur_df["returned_value"].fillna(0, inplace=True)

pre_df = pre_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
cur_df = cur_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)


pre_df.drop(["adjusted_value", "instance "], axis=1, inplace=True)
cur_df.drop(["adjusted_value", "instance"], axis=1, inplace=True)

# Data transmutation
pre_responsedf = pre_df.pivot_table(
    index=["reference", "period"],
    columns="question_no",
    values="returned_value",
    aggfunc="first",
).reset_index()

cur_responsedf = cur_df.pivot_table(
    index=["reference", "period"],
    columns="question_no",
    values="returned_value",
    aggfunc="first",
).reset_index()


# Drop all but key columns
key_cols = [
    "reference",
    "period",
    200,
    201,
    211,
    305,
    405,
    406,
    407,
    407,
    408,
    409,
    410,
    501,
    502,
    503,
    504,
    505,
    506,
]

pre_cleandf = pre_responsedf.loc[:, pre_responsedf.columns.intersection(key_cols)]
cur_cleandf = cur_responsedf.loc[:, cur_responsedf.columns.intersection(key_cols)]

# PG mapping
map_dict = dict(zip(mapper["2016 > Form PG"], mapper["2016 > Pub PG"]))
map_dict = {float(i): j for i, j in map_dict.items()}

pre_cleandf[201] = pd.to_numeric(pre_cleandf[201], errors="coerce")
cur_cleandf[201] = pd.to_numeric(cur_cleandf[201], errors="coerce")

pre_cleandf[201].replace(0.0, float("NaN"))
cur_cleandf[201].replace(0.0, float("NaN"))

pre_cleandf.replace({201: map_dict}, inplace=True)
cur_cleandf.replace({201: map_dict}, inplace=True)

# Identify long and short form formats for filtering


# Remove rows of data for imputation
pre_sample = pre_cleandf.sample(frac=0.2)
cur_sample = cur_cleandf.sample(frac=0.2)

# Dataset joining
# joined_df = pd.merge(pre_cleandf,
# cur_cleandf, on=['reference','period',200,201], how='outer')

print("Ended debug")
