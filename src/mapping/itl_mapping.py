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
        responses (Tuple[pd.DataFrame, pd.DataFrame]): The GB & NI responses dataframes
        postcode_mapper (pd.DataFrame): Mapper containing postcodes and regions
        formtype (list): List of the formtypes to run through function

    Returns:
        Tuple(gb_df: pd.DataFrame, ni_df: pd.DataFrame): dfs with the ITL regions joined
    """
    gb_df, ni_df = responses
    # first create itl column
    postcode_mapper = postcode_mapper.rename(columns={"pcd2": pc_col})
    gb_df = join_with_null_check(gb_df, postcode_mapper, "postcode mapper", pc_col)

    # next join the itl mapper to add the region columns
    geo_cols = ["LAU121CD", "ITL221CD", "ITL221NM", "ITL121CD", "ITL121NM"]
    itl_mapper = itl_mapper[geo_cols].rename(columns={"LAU121CD": "itl"})

    # TODO: remove the "warn" parameter when the ITL mapper is fixed
    gb_df = join_with_null_check(gb_df, itl_mapper, "itl mapper", "itl", warn=True)

    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", None)
    # if the ni_df is not empty, add the itl column to it
    if not ni_df.empty:
        ni_df["itl"] = "N92000002"

    return gb_df, ni_df
