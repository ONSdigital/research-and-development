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
    colname_schema_dict = {
        output_schema[column_nm]["old_name"]: column_nm
        for column_nm in output_schema.keys()
    }

    # Create subset dataframe with only the required outputs
    output_df = df[colname_schema_dict.keys()].copy()

    # Rename columns to match the output specification
    output_df.rename(columns=colname_schema_dict, inplace=True)

    # Rearrange to match user defined output order
    output_df = output_df[colname_schema_dict.values()]

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
        "NI": ["YY"],
    }

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


def postcode_topup(mystr: str, target_len: int = 8) -> str:
    """Regulates the number of spaces between the first and the second part of
    a postcode, so that the total length is 8 characters.
    Brings all letters to upper case, in line with the mapper.
    Splits the postcode string into parts, separated by any number of spaces.
    If there are two or more parts, the first two parts are used.
    The third and following parts, if present, are ignored.
    Calculates how many spaces are needed so that the total length is 8.
    If the total length of part 1 and part 2 is already 8, no space will be
    inserted.
    If their total length is more than 8, joins part 1 and part 2 without
    spaces and cuts the tail on the right.
    If there is only one part, keeps the first 8 characters and tops it up with
    spaces on the right if needed.
    Empty input string would have zero parts and will return a string of
    eight spaces.

    Args:
        mystr (str): Input postcode.
        target_len (int): The desired length of the postcode after topping up.

    Returns:
        str: The postcode topped up to the desired number of characters.
    """
    mystr = mystr.upper()
    parts = mystr.split()
    if len(parts) >= 2:
        part1 = parts[0]
        part2 = parts[1]
        num_spaces = target_len - len(part1) - len(part2)
        if num_spaces >= 0:
            return part1 + " " * num_spaces + part2
        else:
            return (part1 + part2)[:target_len]
    else:
        return mystr[:target_len].ljust(target_len, " ")
