"""Function to run the mapping of foreign ownership (ultfoc)"""

import pandas as pd

from src.mapping import mapping_helpers as hlp


def validate_ultfoc_mapper(ultfoc_mapper: pd.DataFrame) -> None:
    """
    Validate the foreign ownership (ultfoc) mapper.

    Args:
        ultfoc_mapper (pd.DataFrame): The foreign ownership mapper DataFrame.

    Returns:
        pd.DataFrame: The validated foreign ownership mapper DataFrame.
    """
    hlp.mapper_null_checks(ultfoc_mapper, "ultfoc", "ruref", "ultfoc")
    hlp.col_validation_checks(ultfoc_mapper, "ultfoc", "ultfoc", str, 2, True)
    hlp.check_mapping_unique(ultfoc_mapper, "ruref")


def join_fgn_ownership(
    df: pd.DataFrame,
    mapper_df: pd.DataFrame,
    is_northern_ireland: bool = False,
) -> pd.DataFrame:
    """
    Validate and join the foreign ownership (ultfoc) mapper to the responses dataframes.

    Args:
        df (pd.DataFrame): The main DataFrame.
        mapper_df (pd.DataFrame): The mapper DataFrame.

    Returns:
        pd.DataFrame: The combined DataFrame resulting from the left join.
    """
    mapper_df = validate_ultfoc_mapper(mapper_df)

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
