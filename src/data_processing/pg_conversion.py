import pandas as pd
import numpy as np

import logging

pg_logger = logging.getLogger(__name__)


def pg_mapper(
    df: pd.DataFrame,
    mapper: pd.DataFrame,
    target_col: str,
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
    map_dict = dict(zip(mapper[from_col], mapper[to_col]))
    map_dict = {i: j for i, j in map_dict.items()}

    df[target_col] = pd.to_numeric(df[target_col], errors="coerce")
    df[target_col] = df[target_col].replace({0: np.nan})
    df.replace({target_col: map_dict}, inplace=True)
    df[target_col] = df[target_col].astype("category")

    pg_logger.info("Product groups successfully mapped to letters")
    return df
