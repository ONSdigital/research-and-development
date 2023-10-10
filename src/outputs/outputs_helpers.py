import pandas as pd
from src.staging.validation import load_schema, validate_data_with_schema


def create_output_df(df: pd.DataFrame, schema: str) -> pd.DataFrame:
    """Creates the dataframe for outputs with
    the required columns. The naming of the columns comes
    from the schema provided.

    Args:
        df (pd.DataFrame): Dataframe containing all columns
        schema (str): Toml schema containing the old and new
        column names for the outputs

    Returns:
        (pd.DataFrame): A dataframe consisting of only the
        required short form output data
    """
    # Load schema using pre-built function
    output_schema = load_schema(schema)

    # Create dict of current and required column names
    colname_dict = {
        output_schema[column_nm]["old_name"]: column_nm
        for column_nm in output_schema.keys()
    }

    # Create subset dataframe with only the required outputs
    output_df = df[df.columns.intersection(colname_dict.keys())]

    # Rename columns to match the output specification
    output_df.rename(
        columns={key: colname_dict[key] for key in colname_dict}, inplace=True
    )

    # Rearrange to match user defined output order
    output_df = output_df[colname_dict.values()]

    # Validate datatypes before output
    validate_data_with_schema(output_df, schema)

    return output_df