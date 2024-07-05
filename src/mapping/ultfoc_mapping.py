"""Function to run the mapping of foreign ownership (ultfoc)"""

import pandas as pd

def join_fgn_ownership(
    df: pd.DataFrame,
    mapper_df: pd.DataFrame,
    is_northern_ireland: bool = False,
) -> pd.DataFrame:
    """
    Combine two DataFrames using a left join based on specified columns.

    Args:
        df (pd.DataFrame): The main DataFrame.
        mapper_df (pd.DataFrame): The mapper DataFrame.

    Returns:
        pd.DataFrame: The combined DataFrame resulting from the left join.
    """

    if is_northern_ireland:
        mapped_ni_df = df.rename(columns={"foc": "ultfoc"})
        mapped_ni_df["ultfoc"] = mapped_ni_df["ultfoc"].fillna("GB")
        mapped_ni_df["ultfoc"] = mapped_ni_df["ultfoc"].replace("", "GB")
        return mapped_ni_df

    else:
        mapped_df = df.merge(
            mapper_df,
            how="left",
            left_on="reference",
            right_on="ruref",
        )
        mapped_df.drop(columns=["ruref"], inplace=True)
        mapped_df["ultfoc"] = mapped_df["ultfoc"].fillna("GB")
        mapped_df["ultfoc"] = mapped_df["ultfoc"].replace("", "GB")
        return mapped_df
