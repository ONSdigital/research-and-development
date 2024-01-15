import pandas as pd
import logging
import numpy as np

PgLogger = logging.getLogger(__name__)


def sic_to_pg_mapper(
    df: pd.DataFrame,
    sicmapper: pd.DataFrame,
    pg_column: str = "201",
    sic_column: str = "rusic",
    from_col: str = "SIC 2007_CODE",
    to_col: str = "2016 > Form PG",
):
    """Map from SIC code to PG numeric code where PG numeric is null.

    Example initial dataframe:
        reference | 201     | rusic
    --------------------------------
        1         | 53      | 2500   
        2         | NaN     | 1600
        3         | NaN     | 4300

    returned dataframe:
        reference | 201     | rusic
    --------------------------------
        1         | 53      | 2500   
        2         | 45      | 1600
        3         | 38      | 4300

    Args:
        df (pd.DataFrame): The dataset containing all the PG numbers.
        sicmapper (pd.DataFrame): The SIC to pg numeric mapper.
        sic_column (str, optional): The column containing the SIC numbers.
        from_col (str, optional): The column in the mapper that is used to map from.
        to_col (str, optional): The column in the mapper that is used to map to.

    Returns:
        pd.DataFrame: A dataframe with all target column values mapped
    """

    df = df.copy()

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
        raise Exception("Errors in the SIC to PG numeric mapper.")
    
    # Map to the target column using the dictionary, null values only
    df.loc[df[pg_column].isnull(), pg_column] = (
        df.loc[df[pg_column].isnull(), sic_column].map(map_dict)
    )

    PgLogger.info("Product group nulls successfully mapped from SIC.")

    return df


def pg_to_pg_mapper(
    df: pd.DataFrame,
    mapper: pd.DataFrame,
    pg_column: str = "201",
    from_col: str = "pg_numeric",
    to_col: str = "pg_alpha",
):
    """Map from PG numeric to PG alpha-numeric and create a new column.

    The mapper used is from a file named pg_num_alpha.csv

    The product group column (default: column 201) is copied to a new column, 
    "pg_numeric", and then the original column is mapped from numeric to alpha-numeric.

    Example initial dataframe:
        reference | 201     
    ----------------------
        1         | 53    
        2         | 43     
        3         | 33    

    returned dataframe:
        reference | 201     | pg_numeric
    ------------------------------------
        1         | AA      | 33
        2         | B       | 43
        3         | E       | 53


    Args:
        df (pd.DataFrame): The dataframe requiring mapping
        mapper (pd.DataFrame): the PG numeric to alpha-numeric mapper
        pg_column (str, optional): The column we want to convert (default 201).
        from_col (str, optional): The column in the mapper that is used to map from.
        to_col (str, optional): The column in the mapper that is used to map to.

    Returns:
        pd.DataFrame: A dataframe with all target column values mapped
    """

    df = df.copy()

    # Copy the numeric PG column to a new column
    df["pg_numeric"] = df[pg_column].copy()

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
        raise Exception("Errors in the PG numeric to alpha-numeric mapper.")

    df[pg_column] = df[pg_column].map(map_dict)

    # Then convert the pg column and the new column to categorigal datatypes
    df = df.astype({pg_column: "category", "pg_numeric": "category"})

    PgLogger.info("Numeric product groups successfully mapped to letters.")

    return df


def run_pg_conversion(
    df: pd.DataFrame,
    pg_num_alpha: pd.DataFrame,
    sic_pg_num: pd.DataFrame,
    pg_column: str = "201",
):
    """Run the product group (PG) mapping functions.

    Args:
        df (pd.DataFrame): Dataframe of full responses data
        pg_num_alpha (pd.DataFrame): Mapper from numeric to alpha-numeric PG.
        pg_column: The original product group column, default 201

    Returns:
        (pd.DataFrame): Dataframe with mapped values
    """
    # Where product group is null, map it from SIC.
    df = sic_to_pg_mapper(df, sic_pg_num, pg_column)

    # PG numeric to alpha_numeric mapping for long forms
    df = pg_to_pg_mapper(df, pg_num_alpha, pg_column)

    return df
