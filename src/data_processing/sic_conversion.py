from src.utils.hdfs_mods import read_hdfs_csv
import pandas as pd
import random
import string
from govsic import SIC
import govsic
from typing import Callable, Dict

previous_period_path = (
    "/ons/rdbe_dev/BERD_V7_Anonymised/qv_BERD_202012_qv6_reformatted.csv"
)
current_period_path = (
    "/ons/rdbe_dev/BERD_V7_Anonymised/qv_BERD_202112_qv6_reformatted.csv"
)

sic_mapper_file = "data/external/SIC07 to PG Conversion - From 2016 Data .csv"
pg_conversion_df = pd.read_csv(
    sic_mapper_file, usecols=["2016 > Form PG", "2016 > Pub PG"]
)

print(pg_conversion_df.sample(10))

previous_df = read_hdfs_csv(previous_period_path)
current_df = read_hdfs_csv(current_period_path)


def generate_id(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_uppercase, k=length))


def main1() -> None:
    data = pd.DataFrame(
        {
            "id": [generate_id() for _ in range(3)],
            "classification": ["01430", "56302", "98200"],
        }
    )

    data["classification"] = data["classification"].map(SIC)
    data["section"] = data["classification"].map(lambda sic: sic.section)

    example: SIC = data["classification"].values[0]

    print(data.head(), example.summary(), sep="\n\n")


TMapping = Dict[str, Callable[[govsic.SIC], str]]


DATA = pd.DataFrame(
    {"id": [generate_id() for _ in range(3)], "sic": ["01430", "20150", "26701"]}
)


def main2() -> None:
    DATA["sic"] = DATA["sic"].map(govsic.SIC)  # type: ignore

    mapping: TMapping = {
        "description": lambda sic: sic.description[0],
        "2-digit": lambda sic: str(sic.code)[:2],
        "section": lambda sic: sic.section,
        "section description": lambda sic: govsic.SECTIONS[sic.section].description,
    }

    for column, function in mapping.items():
        DATA[column] = DATA["sic"].map(function)  # type: ignore

    print(DATA.head())


if __name__ == "__main__":
    main1()
    main2()
