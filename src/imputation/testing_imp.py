import pandas as pd
from src.utils.helpers import Config_settings

conf_obj = Config_settings()
config = conf_obj.config_dict

# Check the environment switch
network_or_hdfs = config["global"]["network_or_hdfs"]

try:
    import pydoop.hdfs as hdfs

    # from src.utils.hdfs_mods import read_hdfs_csv, write_hdfs_csv
    HDFS_AVAILABLE = True
except ImportError:
    HDFS_AVAILABLE = False


def read_hdfs_csv(filepath: str) -> pd.DataFrame:
    """Reads a csv from DAP into a Pandas Dataframe
    Args:
        filepath (str): Filepath (Specified in config)

    Returns:
        pd.DataFrame: Dataframe created from csv
    """
    # Open the file in read mode inside Hadoop context
    with hdfs.open(filepath, "r") as file:
        # Import csv file and convert to Dataframe
        df_imported_from_hdfs = pd.read_csv(
            file,
            na_values="                                                                ",  # noqa
        )

    return df_imported_from_hdfs


if network_or_hdfs == "network":
    HDFS_AVAILABLE = False

    pre_path = "R:/BERD Results System Development 2023/DAP_emulation/BERD_V7_Anonymised/qv_BERD_202012_qv6_reformatted.csv"  # noqa
    cur_path = "R:/BERD Results System Development 2023/DAP_emulation/BERD_V7_Anonymised/qv_BERD_202112_qv6_reformatted.csv"  # noqa

    pre_df = pd.read_csv(
        pre_path,
        na_values="                                                                ",
    )
    cur_df = pd.read_csv(
        cur_path,
        na_values="                                                                ",
    )


elif network_or_hdfs == "hdfs":
    HDFS_AVAILABLE = False

    pre_path = "/ons/rdbe_dev/BERD_V7_Anonymised/qv_BERD_202012_qv6_reformatted.csv"
    cur_path = "/ons/rdbe_dev/BERD_V7_Anonymised/qv_BERD_202112_qv6_reformatted.csv"

    pre_df = read_hdfs_csv(pre_path)
    cur_df = read_hdfs_csv(cur_path)

else:
    raise ImportError


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

print(pre_responsedf.head(10))
print(cur_responsedf.head(10))


print("Ended debug")
