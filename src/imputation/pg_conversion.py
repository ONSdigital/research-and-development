import pandas as pd
import logging

pg_logger = logging.getLogger(__name__)


def pg_to_pg_mapper(
    df: pd.DataFrame,
    mapper: pd.DataFrame,
    target_col: str = "201",
    from_col: str = "2016 > Form PG",
    to_col: str = "2016 > Pub PG",
):
    """This function maps all values in one column to another column
    using a mapper file.
    The default this is used for is PG numeric to letter conversion.

    Args:
        df (pd.DataFrame): The dataset containing all the PG numbers
        mapper (pd.DataFrame): The mapper dataframe loaded using custom function
        target_col (str): The column we want to convert (product_group)
        from_col (str, optional): The column in the mapper that is used to map from.
        Defaults to "2016 > Form PG".
        to_col (str, optional): The column in the mapper that is used to map to.
        Defaults to "2016 > Pub PG".

    Returns:
        pd.DataFrame: A dataframe with all target column values mapped
    """

    mapdf = mapper.drop_duplicates(subset=[from_col, to_col], keep="first")
    map_errors = mapdf["2016 > Form PG"][
        mapdf["2016 > Form PG"].duplicated(keep=False)
    ].unique()

    print(map_errors)

    pg_logger.error(
        (
            f"The following product groups are trying to map to multiple letters: "
            f"{map_errors}"
        )
    )

    map_dict = dict(zip(mapper[from_col], mapper[to_col]))
    map_dict = {i: j for i, j in map_dict.items()}

    df[target_col] = pd.to_numeric(df[target_col], errors="coerce")
    df[target_col] = df[target_col].map(map_dict)
    df[target_col] = df[target_col].astype("category")

    pg_logger.info("Product groups successfully mapped to letters")

    return df


def sic_to_pg_mapper(
    df: pd.DataFrame,
    sicmapper: pd.DataFrame,
    target_col: str = "201",
    sic_column: str = "rusic",
    from_col: str = "SIC 2007_CODE",
    to_col: str = "2016 > Pub PG",
):
    """This function maps all values in one column to another column
    using a mapper file.

    The default this is used for is PG numeric to letter conversion.

    Args:
        df (pd.DataFrame): The dataset containing all the PG numbers
        mapper (pd.DataFrame): The mapper dataframe loaded using custom function
        target_col (str): The column we want to convert (product_group)
        from_col (str, optional): The column in the mapper that is used to map from.
        Defaults to "SIC 2007_CODE".
        to_col (str, optional): The column in the mapper that is used to map to.
        Defaults to "2016 > Pub PG".

    Returns:
        pd.DataFrame: A dataframe with all target column values mapped
    """

    mapdf = sicmapper.drop_duplicates(subset=[from_col, to_col], keep="first")
    map_errors = mapdf["SIC 2007_CODE"][
        mapdf["SIC 2007_CODE"].duplicated(keep=False)
    ].unique()

    print(map_errors)

    pg_logger.error(
        f"The following SIC numbers are trying to map to multiple letters: {map_errors}"
    )

    map_dict = dict(zip(sicmapper["SIC 2007_CODE"], sicmapper["2016 > Pub PG"]))
    map_dict = {i: j for i, j in map_dict.items()}

    df[sic_column] = pd.to_numeric(df[sic_column], errors="coerce")
    df[target_col] = df[sic_column].map(map_dict)
    df[target_col] = df[target_col].astype("category")

    pg_logger.info("SIC numbers successfully mapped to PG letters")

    return df
