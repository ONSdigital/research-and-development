"""Prepare the survey data for short form microdata output."""
import pandas as pd
import numpy as np
from src.staging.validation import load_schema, validate_data_with_schema


def create_new_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Created blank columns & year column for short form output

    Blank columns are created for short form output that are required
    but will currently be supplied empty.
    The 'period_year' column is added containing the year in form 'YYYY'.

    Args:
        df (pd.DataFrame): The main dataframe to be used for short form output.

    Returns:
        pd.DataFrame: returns short form output data frame with added new cols
    """
    columns_names = [
        "freeze_id",
        "inquiry_id",
        "period_contributor_id",
        "post_code",
        "product_group",
        "ua_county",
        "foreign_owner",
        "sizeband",
    ]

    # Added new columns from column_names list
    for col in columns_names:
        df[col] = np.nan

    # Extracted the year from period and crated new columns 'period_year'
    df["period_year"] = df["period"].astype("str").str[:4]

    return df


def create_headcount_cols(
    df: pd.DataFrame,
    round_val: int = 4,
) -> pd.DataFrame:
    """Create new columns with headcounts for civil and defence.

    Column '705' contains the total headcount value, and
    from this the headcount values for civil and defence are calculated
    based on the percentages of civil and defence in columns '706' (civil)
    and '707' (defence). Note that columns '706' and '707' measure different
    things to '705' so will not in general total to the '705' value.

    Args:
        df (pd.DataFrame): The survey dataframe being prepared for
            short form output.
        round_val (int): The number of decimal places for rounding.

    Returns:
        pd.DataFrame: The dataframe with extra columns for civil and
            defence headcount values.
    """
    # Use np.where to avoid division by zero.
    df["headcount_civil"] = np.where(
        df["706"] + df["707"] != 0,  # noqa
        df["705"] * df["706"] / (df["706"] + df["707"]),
        0,
    )

    df["headcount_defence"] = np.where(
        df["706"] + df["707"] != 0,  # noqa
        df["705"] * df["707"] / (df["706"] + df["707"]),
        0,
    )

    df["headcount_civil"] = round(df["headcount_civil"], round_val)
    df["headcount_defence"] = round(df["headcount_defence"], round_val)

    return df

def combine_dataframes(main_df: pd.DataFrame, mapper_df: pd.DataFrame) -> pd.DataFrame:
    """
    Combine two DataFrames using a left join based on specified columns.

    Args:
        main_df (pd.DataFrame): The main DataFrame.
        mapper_csv_path (pd.DataFrame): The mapper DataFrame.

    Returns:
        pd.DataFrame: The combined DataFrame resulting from the left join.
    """
    try:
        # Perform left join
        combined_df = main_df.merge(
            mapper_df, how="left", left_on="reference", right_on="ruref"
        )
        combined_df.drop(columns=["ruref"], inplace=True)

        return combined_df

    except Exception as e:
        raise ValueError(
            "An error occurred while combining main_df and mapper_df: " + str(e)
        )



def map_sizebands(
    df: pd.DataFrame,
):
    """Generate sizebands from the frozen employent column

    Args:
        df (pd.DataFrame): The original dataframe

    Returns:
        (pd.DataFrame): The dataframe with the sizebands column added
    """
    # Create a dictionary of sizeband parameters
    sizeband_dict = {
        1: {"min": 0, "max": 9},
        2: {"min": 10, "max": 99},
        3: {"min": 20, "max": 49},
        4: {"min": 50, "max": 99},
        5: {"min": 100, "max": 249},
        6: {"min": 250, "max": np.inf},
    }

    # Create empty column
    df["sizeband"] = np.nan

    # Create conditions for sizebands
    conditions = [
        (sizeband_dict[key]["min"] <= df["frozenemployment"])
        & (df["frozenemployment"] <= sizeband_dict[key]["max"])
        for key in sizeband_dict.keys()
    ]
    decisions = [key for key in sizeband_dict.keys()]

    # Apply the sizebands
    for i in range(len(conditions)):
        df.loc[conditions[i], "sizeband"] = decisions[i]

    # Convert datatype to int
    df["sizeband"] = df["sizeband"].astype("Int64")

    return df

def create_cora_status_col(df, mapper_df, main_col="statusencoded"):
    """_summary_

    Args:
        df (pd.DataFrame): main data containing responses
        mapper_df (pd.DataFrame): mapper with cora status equivalents
        main_col (str, optional): Defaults to "statusencoded".

    Returns:
        df: main data with cora status column added
    """
    
    # Create hardcoded dictionary for mapping if csv is not used
    cora_dict = {"statusencoded": 	[100,101,102,200,201,210,211,302,303,304,309],
        "form_status": [200, 100, 1000, 400, 500, 600, 800, 1200, 1300, 900, 1400]}

    # convert mapper df to dictionary
    mapper_dict = dict(zip(mapper_df[main_col], mapper_df["form_status"]))

    # Create a new column by mapping values from main_col using the
    # mapper dictionary
    df["form_status"] = df[main_col].map(mapper_dict)
    
    return df


def run_shortform_prep(
    df: pd.DataFrame,
    cora_mapper: pd.DataFrame,
    ultfoc_mapper: pd.DataFrame,
    round_val: int = 4,
) -> pd.DataFrame:
    """Prepare data for short form output.

    Perform various steps to create new columns and modify existing
    columns to prepare the main survey dataset for short form
    micro data output.

    Args:
        df (pd.DataFrame): The survey dataframe being prepared for
            short form output.
        round_val (int): The number of decimal places for rounding.

    Returns:
        pd.DataFrame: The dataframe prepared for short form output.
    """
    
    # Create combined ownership column using mapper
    df = combine_dataframes(df, ultfoc_mapper)

    # Filter for short-forms
    df = df.loc[df["formtype"] == "0006"]
    
    # Map to the CORA statuses from the statusencoded column
    df = create_cora_status_col(df,cora_mapper)

    # Filter for CORA statuses [600, 800]
    df = df.loc[df["form_status"].isin(["600", "800"])]

    # create new columns and create a 'year' column
    df = create_new_cols(df)

    # create columns for headcounts for civil and defense
    df = create_headcount_cols(df, round_val)

    # Map the sizebands based on frozen employment
    df = map_sizebands(df)
    
    return df


def create_shortform_df(df: pd.DataFrame, schema: str) -> pd.DataFrame:
    """Creates the shortform dataframe for outputs with
    the required columns. The naming

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

    # Create empty dataframe for outputs
    output_df = pd.DataFrame()

    # Add required columns from schema to output
    for key in colname_dict.keys():
        output_df[colname_dict[key]] = df[key]

    # Validate datatypes before output
    validate_data_with_schema(output_df, schema)

    return output_df
