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
        (dict): A dictionary of region codes for four UK counties, GB and UK
    """
    regions = {
        "England": ["AA", "BA", "BB", "DC", "ED", "FE", "GF", "GG", "HH", "JG", "KJ"],
        "Wales": ["WW"],
        "Scotland": ["XX"],
        "NI": ["YY"], }

    regions["GB"] = regions["England"] + regions["Wales"] + regions["Scotland"]
    regions["UK"] = regions["GB"] + regions["NI"]
    return regions
