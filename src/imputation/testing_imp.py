import pandas as pd
from src.utils.helpers import Config_settings
import math
import numpy as np
from src.imputation import imputation as imp
from src.data_processing import spp_snapshot_processing as spp

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
    cur_con_path = "/ons/rdbe_dev/BERD_V7_Anonymised/cp_BERD_202112_cp3.csv"
    pre_con_path = "/ons/rdbe_dev/BERD_V7_Anonymised/cp_BERD_202012_cp3.csv"

    with hdfs.open(pre_path, "r") as file:
        # Import csv file and convert to Dataframe
        pre_df = pd.read_csv(
            file,
            na_values=[
                "                                                                ",
                math.nan,
            ],  # noqa
        )

    with hdfs.open(cur_path, "r") as file:
        # Import csv file and convert to Dataframe
        cur_df = pd.read_csv(
            file,
            na_values=[
                "                                                                ",
                math.nan,
            ],  # noqa
        )

    with hdfs.open(map_path, "r") as file:
        # Load SIC to PG mapper
        mapper = pd.read_csv(
            file, usecols=["2016 > Form PG", "2016 > Pub PG"]
        ).squeeze()

    with hdfs.open(pre_con_path, "r") as file:
        # Import csv file and convert to Dataframe
        pre_con_df = pd.read_csv(
            file,
            na_values=[
                "                                                                ",
                math.nan,
            ],  # noqa
        )

    with hdfs.open(cur_con_path, "r") as file:
        # Import csv file and convert to Dataframe
        cur_con_df = pd.read_csv(
            file,
            na_values=[
                "                                                                ",
                math.nan,
            ],  # noqa
        )
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

pre_responsedf = spp.full_responses(pre_con_df, pre_df)
cur_responsedf = spp.full_responses(cur_con_df, cur_df)


# pre_responsedf = pre_df.pivot_table(
#     index=["reference", "period"],
#     columns="question_no",
#     values="returned_value",
#     aggfunc="first",
# ).reset_index()

# cur_responsedf = cur_df.pivot_table(
#     index=["reference", "period"],
#     columns="question_no",
#     values="returned_value",
#     aggfunc="first",
# ).reset_index()


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

# # Identify long and short form formats for filtering and filter
pre_shortform_df = pre_responsedf.loc[~pre_responsedf[705].isnull()]
cur_shortform_df = cur_responsedf.loc[~cur_responsedf[705].isnull()]

pre_responsedf = pre_responsedf.loc[pre_responsedf[705].isnull()]
cur_responsedf = cur_responsedf.loc[cur_responsedf[705].isnull()]

pre_cleandf = pre_responsedf.loc[:, pre_responsedf.columns.intersection(key_cols)]
cur_cleandf = cur_responsedf.loc[:, cur_responsedf.columns.intersection(key_cols)]

# Casting Data Types
datatypes = {201: "Int64", 211: "Int64", 305: "Int64"}

dtypes2 = {
    405: "float",
    406: "float",
    407: "float",
    408: "float",
    409: "float",
    410: "float",
    501: "float",
    502: "float",
    503: "float",
    504: "float",
    505: "float",
    506: "float",
}

# 200 cleaning
pre_cleandf[200] = pre_cleandf[200].apply(
    lambda v: str(v) if str(v) != "nan" else np.nan
)
pre_cleandf[200] = pre_cleandf[200].replace({"0": np.nan, "0.0": np.nan})
pre_cleandf[200] = pre_cleandf[200].astype("category")

# Casting datatypes
pre_cleandf[list(datatypes.keys())] = (
    pre_cleandf[datatypes.keys()].astype(float).astype(datatypes)
)
pre_cleandf[list(dtypes2.keys())] = pre_cleandf[dtypes2.keys()].astype(dtypes2)

# 200 cleaning
cur_cleandf[200] = cur_cleandf[200].apply(
    lambda v: str(v) if str(v) != "nan" else np.nan
)
cur_cleandf[200] = cur_cleandf[200].replace({"0": np.nan, "0.0": np.nan})
cur_cleandf[200] = cur_cleandf[200].astype("category")

# Casting datatypes
cur_cleandf[list(datatypes.keys())] = (
    cur_cleandf[datatypes.keys()].astype(float).astype(datatypes)
)
cur_cleandf[list(dtypes2.keys())] = cur_cleandf[dtypes2.keys()].astype(dtypes2)


# PG mapping
map_dict = dict(zip(mapper["2016 > Form PG"], mapper["2016 > Pub PG"]))
map_dict = {i: j for i, j in map_dict.items()}

pre_cleandf[201] = pd.to_numeric(pre_cleandf[201], errors="coerce")
cur_cleandf[201] = pd.to_numeric(cur_cleandf[201], errors="coerce")

pre_cleandf[201] = pre_cleandf[201].replace({0: np.nan})
cur_cleandf[201] = cur_cleandf[201].replace({0: np.nan})

pre_cleandf.replace({201: map_dict}, inplace=True)
cur_cleandf.replace({201: map_dict}, inplace=True)

pre_cleandf[201] = pre_cleandf[201].astype("category")
cur_cleandf[201] = cur_cleandf[201].astype("category")


# Remove rows that need imputing (Status emulation)
vars = [211, 305, 405, 406, 407, 408, 409, 410, 501, 502, 503, 504, 505, 506]
cur_sample10 = cur_cleandf.sample(frac=0.1, random_state=42)
cur_cleandf.loc[cur_sample10.index, vars] = [np.nan for i in range(len(vars))]

# Convert columns to strings
pre_cleandf.columns = pre_cleandf.columns.astype("str")
cur_cleandf.columns = cur_cleandf.columns.astype("str")

# Dataset joining
joined_df = pd.merge(
    pre_cleandf,
    cur_cleandf,
    on=["reference"],
    how="outer",
    suffixes=(
        f"_{str(pre_cleandf['period'][0])[:4]}",
        f"_{str(cur_cleandf['period'][0])[:4]}",
    ),
)

# IMPUTATION MVP

# Carry forward 200 and 201

# Create Imputation Classes

df = imp.create_imp_class_col(joined_df, "200_2020", "201_2020", "2020_class")
df = imp.create_imp_class_col(joined_df, "200_2021", "201_2021", "2021_class")

ind = df[df["2020_class"] == "nannan"].index
df.drop(ind, inplace=True)
ind = df[df["2021_class"] == "nannan"].index
df.drop(ind, inplace=True)

# Find matched pairs

filtered_df = imp.filter_same_class(df, "2021", "2020")
non_matches = df[df["2021_class"] != df["2020_class"]]

vars = [str(i) for i in vars]
matches = dict.fromkeys(vars)
mp_counts = dict.fromkeys(vars)

for i in vars:
    matched_pairs_df = imp.filter_pairs(filtered_df, i, "2021", "2020")
    matches.update({i: matched_pairs_df})
    mp_counts.update({i: len(matched_pairs_df)})

# Calculate growth ratio
for i in vars:
    imp.calc_growth_ratio(i, filtered_df, "2021", "2020")

# Trimming
print("Ended debug")
