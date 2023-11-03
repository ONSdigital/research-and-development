import pandas as pd


def create_output_df(df: pd.DataFrame, output_schema: dict) -> pd.DataFrame:
    """Creates the dataframe for outputs with
    the required columns. The naming of the columns comes
    from the schema provided.

    Args:
        df (pd.DataFrame): Dataframe containing all columns
        output_schema (str): Toml schema containing the old and new
        column names for the outputs

    Returns:
        (pd.DataFrame): A dataframe consisting of only the
        required short form output data
    """

    # Create dict of current and required column names
    colname_dict = {
        output_schema[column_nm]["old_name"]: column_nm
        for column_nm in output_schema.keys()
    }

    # Create subset dataframe with only the required outputs
    output_df = df[df.columns.intersection(colname_dict.keys())].copy()

    # Rename columns to match the output specification
    output_df.rename(
        columns={key: colname_dict[key] for key in colname_dict}, inplace=True
    )

    # Rearrange to match user defined output order
    output_df = output_df[colname_dict.values()]

    return output_df


def regions() -> dict:
    """Creates a dictionary of UK regions.

    Args:
        None

    Returns:
        (dict): A dictionary of region codes for England, Wales, Scotland, GB and UK
    """
    regions = {
        "England": ["AA", "BA", "BB", "DC", "ED", "FE", "GF", "GG", "HH", "JG", "KJ"],
        "Wales": ["WW"],
        "Scotland": ["XX"],
        "NI": ["YY"], }

    regions["GB"] = regions["England"] + regions["Wales"] + regions["Scotland"]
    regions["UK"] = regions["GB"] + regions["NI"]
    return regions


def aggregate_output(
    df: pd.DataFrame,
    key_cols: list,
    value_cols: list,
    agg_method: str = "sum",
) -> pd.DataFrame:

    """Groups the datadrame by key columns and aggregates the value columns
    using a specified aggregation method.

    Args:
        df (pd.DataFrame): Dataframe containing all columns
        key_cols (list): List of key column names
        value_cols (list): List of value column names


    Returns:
        df_agg (pd.DataFrame): A dataframe containing key columns and aggregated values
    """

    # Check what columns are available
    available_cols = df.columns.tolist()
    my_keys = [c for c in key_cols if c in available_cols]
    my_values = [c for c in value_cols if c in available_cols]

    # Dictionary for aggregation
    agg_dict = {x: agg_method for x in my_values}

    # Groupby and aggregate
    df_agg = df.groupby(my_keys).agg(agg_dict).reset_index()

    return df_agg


def create_period_year(df: pd.DataFrame) -> pd.DataFrame:
    """Created year column for short form output

    The 'period_year' column is added containing the year in form 'YYYY'.

    Args:
        df (pd.DataFrame): The main dataframe to be used for short form output.

    Returns:
        pd.DataFrame: returns short form output data frame with added new col
    """

    # Extracted the year from period and crated new columns 'period_year'
    df["period_year"] = df["period"].astype("str").str[:4]

    return df
