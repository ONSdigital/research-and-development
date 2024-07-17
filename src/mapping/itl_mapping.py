"""Code to join the ITL regions onto the full dataframe using the mapper provided."""
import pandas as pd
from typing import Tuple

from src.mapping.mapping_helpers import join_with_null_check


def join_itl_regions(
    responses: Tuple[pd.DataFrame, pd.DataFrame],
    postcode_mapper: pd.DataFrame,
    itl_mapper: pd.DataFrame,
    is_ni: bool = False,
    pc_col: str = "postcodes_harmonised",
):
    """Joins the itl regions onto the full dataframe using the mapper provided

    Args:
        df (pd.DataFrame): Full dataframe
        postcode_mapper (pd.DataFrame): Mapper containing postcodes and regions
        formtype (list): List of the formtypes to run through function

    Returns:
        Tuple(gb_df: pd.DataFrame, ni_df: pd.DataFrame): dfs with the ITL regions joined
    """
    gb_df, ni_df = responses
    # first create itl column
    postcode_mapper = postcode_mapper.rename(columns={"pcd2": pc_col})
    gb_df = join_with_null_check(gb_df, postcode_mapper, "postcode mapper", pc_col)

    if ni_df is not None:
        ni_df["itl"] = "N92000002"

    # next join the itl mapper
    geo_cols = ["LAU121CD", "ITL221CD", "ITL221NM", "ITL121CD", "ITL121NM"]
    itl_mapper = itl_mapper[geo_cols].rename(columns={"LAU121CD": "itl"})

    # TODO: remove the "warn" parameter when the ITL mapper is fixed
    gb_df = join_with_null_check(gb_df, itl_mapper, "itl mapper", "itl", warn=True)
    if ni_df is not None:
        ni_df = join_with_null_check(ni_df, itl_mapper, "itl mapper", "itl", warn=True)

    return gb_df, ni_df
