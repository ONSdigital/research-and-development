"""Code to join the ITL regions onto the full dataframe using the mapper provided."""
import pandas as pd

from src.mapping.mapping_helpers import join_with_null_check


def join_itl_regions(
    df: pd.DataFrame,
    postcode_mapper: pd.DataFrame,
    is_ni: bool = False,
    postcode_col: str = "postcodes_harmonised",
):
    """Joins the itl regions onto the full dataframe using the mapper provided

    Args:
        df (pd.DataFrame): Full dataframe
        postcode_mapper (pd.DataFrame): Mapper containing postcodes and regions
        formtype (list): List of the formtypes to run through function

    Returns:
        df: Dataframe with column "ua_county" for regions
    """
    if not is_ni:
        postcode_mapper = postcode_mapper.rename(columns={"pcd2": postcode_col})
        df = join_with_null_check(df, postcode_mapper, "postcode mapper", postcode_col)

    else:
        df["itl"] = "N92000002"

    return df
