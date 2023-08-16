import pandas as pd
import logging

PgLogger = logging.getLogger(__name__)


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
        target_col (str, optional): The column we want to convert (product_group).
        from_col (str, optional): The column in the mapper that is used to map from.
        to_col (str, optional): The column in the mapper that is used to map to.

    Returns:
        pd.DataFrame: A dataframe with all target column values mapped
    """
    # Create list of PGs that have one-to-many cardinality
    mapdf = mapper.drop_duplicates(subset=[from_col, to_col], keep="first")
    map_errors = (
        mapdf[from_col][mapdf[from_col].duplicated(keep=False)].unique().tolist()
    )
    # Log the conflicts for the user to fix
    if map_errors:
        PgLogger.error(
            (
                f"The following product groups are trying to map to multiple letters: "
                f"{map_errors}"
            )
        )
    # Create a mapping dictionary from the 2 columns
    map_dict = dict(zip(mapper[from_col], mapper[to_col]))
    map_dict = {i: j for i, j in map_dict.items()}
    # Flag all PGs that don't have a corresponding map value
    mapless_errors = []
    for key, value in map_dict.items():
        if str(value) == "nan":
            mapless_errors.append(key)

    if mapless_errors:
        PgLogger.error(
            f"Mapping doesnt exist for the following product groups: {mapless_errors}"
        )
    # Map using the dictionary taking into account the null values.
    # Then convert to categorigal datatype
    df[target_col] = pd.to_numeric(df[target_col], errors="coerce")
    # TODO: add in some error handling and cleaning here.
    # See ticket RDRP-386
    df[target_col] = df[target_col].map(map_dict)
    df[target_col] = df[target_col].astype("category")

    PgLogger.info("Product groups successfully mapped to letters")

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
        df (pd.DataFrame): The dataset containing all the PG numbers.
        sicmapper (pd.DataFrame): The mapper dataframe loaded using custom function.
        target_col (str, optional): The column we want to convert (product_group).
        sic_column (str, optional): The column containing the SIC numbers.
        from_col (str, optional): The column in the mapper that is used to map from.
        to_col (str, optional): The column in the mapper that is used to map to.

    Returns:
        pd.DataFrame: A dataframe with all target column values mapped
    """
    # Create list of SIC numbers that have one-to-many cardinality
    mapdf = sicmapper.drop_duplicates(subset=[from_col, to_col], keep="first")
    map_errors = (
        mapdf[from_col][mapdf[from_col].duplicated(keep=False)].unique().tolist()
    )
    # Log the conflicts for the user to fix
    if map_errors:
        PgLogger.error(
            f"The following SIC numbers are trying to map to multiple letters: {map_errors}"  # noqa
        )
    # Create a mapping dictionary from the 2 columns
    map_dict = dict(zip(sicmapper[from_col], sicmapper[to_col]))
    map_dict = {i: j for i, j in map_dict.items()}
    # Flag all SIC numbers that don't have a corresponding map value
    mapless_errors = []
    for key, value in map_dict.items():
        if str(value) == "nan":
            mapless_errors.append(key)

    if mapless_errors:
        PgLogger.error(
            f"Mapping doesnt exist for the following SIC numbers: {mapless_errors}"
        )
    # Map to the target column using the dictionary taking into account the null values.
    # Then convert to categorigal datatype
    df[sic_column] = pd.to_numeric(df[sic_column], errors="coerce")
    df[target_col] = df[sic_column].map(map_dict)
    df[target_col] = df[target_col].astype("category")

    PgLogger.info("SIC numbers successfully mapped to PG letters")

    return df
