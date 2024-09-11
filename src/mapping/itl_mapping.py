"""Code to join the ITL regions onto the full dataframe using the mapper provided."""
import pandas as pd

from src.mapping.mapping_helpers import join_with_null_check


def join_itl_regions(
    df: pd.DataFrame,
    postcode_mapper: pd.DataFrame,
    itl_mapper: pd.DataFrame,
    config: dict,
    pc_col: str = "postcodes_harmonised",
    warn_only: bool = False,
) -> pd.DataFrame:
    """Joins the itl regions onto the full dataframe using the mapper provided.

    First, the itl column is added to the dataframe by joining the postcode_mapper.
    Then the itl mapper is joined to add the region columns.

    Args:
        df (pd.DataFrame): The BERD responses dataframes
        postcode_mapper (pd.DataFrame): Mapper containing postcodes and regions
        itl_mapper (pd.DataFrame): Mapper containing ITL regions
        config (dict): Pipeline configuration settings
        pc_col (str, optional): The column name for the postcodes.
        warn_only (bool, optional): Whether to warn only rather than error on nulls.

    Returns:
        pd.DataFrame: the responses dataframe with the ITL regions joined

    Unit Test:
        See [test_itl_mapping](./tests/mapping/test_itl_mapping.py)
    """
    # first create itl column
    postcode_mapper = postcode_mapper.rename(columns={"pcd2": pc_col})
    df = join_with_null_check(df, postcode_mapper, "postcode mapper", pc_col, warn_only)

    # next join the itl mapper to add the region columns
    gb_itl_col = config["mappers"]["gb_itl"]
    geo_cols = [gb_itl_col] + config["mappers"]["geo_cols"]
    itl_mapper = itl_mapper[geo_cols].rename(columns={gb_itl_col: "itl"})

    # TODO: remove the "warn" parameter when the ITL mapper is fixed
    df = join_with_null_check(df, itl_mapper, "itl mapper", "itl", warn=True)

    return df
