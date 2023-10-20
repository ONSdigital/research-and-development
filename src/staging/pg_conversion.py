import pandas as pd
import logging
import numpy as np

PgLogger = logging.getLogger(__name__)


def pg_to_pg_mapper(
    df: pd.DataFrame,
    mapper: pd.DataFrame,
    target_col: str = "product_group",
    pg_column: str = "201",
    from_col: str = "pg_numeric",
    to_col: str = "pg_alpha",
):
    """This function maps all values in one column to another column
    using a mapper file. This is applied to long forms only.
    The default this is used for is PG numeric to letter conversion.

    Args:
        df (pd.DataFrame): The dataset containing all the PG numbers
        mapper (pd.DataFrame): The mapper dataframe loaded using custom function
        target_col (str, optional): The column we output the
        mapped values to (product_group).
        pg_column (str, optional): The column we want to convert (201).
        from_col (str, optional): The column in the mapper that is used to map from.
        to_col (str, optional): The column in the mapper that is used to map to.

    Returns:
        pd.DataFrame: A dataframe with all target column values mapped
    """

    filtered_df = df.copy()

    formtype_cond = filtered_df["formtype"] == "0001"
    filtered_df = filtered_df[formtype_cond]

    # Create a mapping dictionary from the 2 columns
    map_dict = dict(zip(mapper[from_col], mapper[to_col]))
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
    filtered_df[pg_column] = pd.to_numeric(filtered_df[pg_column], errors="coerce")
    filtered_df[target_col] = filtered_df[pg_column].map(map_dict)
    filtered_df[target_col] = filtered_df[target_col].astype("category")

    df.loc[
        filtered_df.index,
        f"{target_col}",
    ] = filtered_df[target_col]

    PgLogger.info("Product groups successfully mapped to letters")

    return df


def sic_to_pg_mapper(
    df: pd.DataFrame,
    sicmapper: pd.DataFrame,
    target_col: str = "product_group",
    sic_column: str = "rusic",
    from_col: str = "sic",
    to_col: str = "pg_alpha",
    formtype: str = "0006",
):
    """This function maps all values in one column to another column
    using a mapper file. This is only applied for short forms and unsampled
    refs.

    The default this is used for is PG numeric to letter conversion.

    Args:
        df (pd.DataFrame): The dataset containing all the PG numbers.
        sicmapper (pd.DataFrame): The mapper dataframe loaded using custom function.
        target_col (str, optional): The column we output the
        mapped values to (product_group).
        sic_column (str, optional): The column containing the SIC numbers.
        from_col (str, optional): The column in the mapper that is used to map from.
        to_col (str, optional): The column in the mapper that is used to map to.

    Returns:
        pd.DataFrame: A dataframe with all target column values mapped
    """

    filtered_df = df.copy()

    formtype_cond = filtered_df["formtype"] == formtype
    filtered_df = filtered_df[formtype_cond]

    # Create a mapping dictionary from the 2 columns
    map_dict = dict(zip(sicmapper[from_col], sicmapper[to_col]))
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
    filtered_df[sic_column] = pd.to_numeric(filtered_df[sic_column], errors="coerce")
    filtered_df[target_col] = filtered_df[sic_column].map(map_dict)
    filtered_df[target_col] = filtered_df[target_col].astype("category")

    df = df.copy()

    df.loc[
        filtered_df.index,
        f"{target_col}",
    ] = filtered_df[target_col]

    PgLogger.info("SIC numbers successfully mapped to PG letters")

    return df


def run_pg_conversion(
    df: pd.DataFrame,
    pg_num_alpha: pd.DataFrame,
    sic_pg_alpha: pd.DataFrame,
    target_col: str = "product_group"
):
    """Run the product group mapping functions and return a
    dataframe with the correct mapping for each formtype.

    Args:
        df (pd.DataFrame): Dataframe of full responses data
        mapper (pd.DataFrame): The mapper file used for PG conversion
        target_col (str, optional): The column to be created
        which stores mapped values.

    Returns:
        (pd.DataFrame): Dataframe with mapped values
    """

    if target_col == "201":
        target_col = "201_mapping"
    else:
        # Create a new column to store PGs
        df[target_col] = np.nan

    # SIC mapping for short forms
    df = sic_to_pg_mapper(df, sic_pg_alpha, target_col=target_col)

    # PG mapping for long forms
    df = pg_to_pg_mapper(df, pg_num_alpha, target_col=target_col)

    # Overwrite the 201 column if target_col = 201
    if target_col == "201_mapping":
        df["201"] = df[target_col]
        df = df.drop(columns=[target_col])

    return df
