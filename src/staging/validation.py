import os
import toml
import pandas as pd
import numpy as np

import logging
from src.utils.wrappers import time_logger_wrap, exception_wrap

# Set up logging
ValidationLogger = logging.getLogger(__name__)


@exception_wrap
def load_schema(file_path: str = "./config/contributors_schema.toml") -> dict:
    """Load the data schema from toml file into a dictionary

    Keyword Arguments:
        file_path -- Path to data schema toml file
        (default: {"./config/contributors_schema.toml"})

    Returns:
        A dict: dictionary containing parsed schema toml file
    """
    # Create bool variable for checking if file exists
    file_exists = os.path.exists(file_path)

    # Check if Data_Schema.toml exists
    if file_exists:
        # Load toml data schema into dictionary if toml file exists
        toml_dict = toml.load(file_path)
    else:
        # Return False if file does not exist
        ValidationLogger.warning(
            "Validation schema does not exist! Path may be incorrect"
        )
        return file_exists

    return toml_dict


@exception_wrap
def check_data_shape(
    data_df: pd.DataFrame,
    contributor_schema: str = "./config/contributors_schema.toml",
    wide_respon_schema: str = "./config/wide_responses.toml",
    raise_error=False,
) -> bool:
    """Compares the shape of the data and compares it to the shape of the toml
    file based off the data schema. Returns true if there is a match and false
    otherwise.

    Keyword Arguments:
        data_df(pd.DataFrame): Pandas dataframe containing data to be checked.
        contributor_schema(str): Path to the schema toml
            (should be in config folder)
        wide_respon_schema(str): Path to the schema toml
            (should be in config folder)
    Returns:
        bool: True if number of columns is as expected, otherwise False
    """
    if not isinstance(data_df, pd.DataFrame):
        raise ValueError(
            f"data_df must be a pandas dataframe, is currently {type(data_df)}."
        )

    cols_match = False

    df_cols_set = set(data_df.columns)

    # Load toml data schemas into dictionary
    toml_string_cont = load_schema(contributor_schema)
    toml_string_response = load_schema(wide_respon_schema)

    # Combine two dicts - with no duplicates
    cont_schema_cols = set(toml_string_cont.keys())
    resp_schema_cols = set(toml_string_response.keys())

    schema_full_col_set = cont_schema_cols.union(resp_schema_cols)
    # Drop the columns which are dropped in SPP processing
    drop_cols_set = {"createdby", "createddate", "lastupdatedby"}
    schema_full_col_set = schema_full_col_set - drop_cols_set

    # Compare length of data dictionary to the data schema
    if len(df_cols_set) == len(schema_full_col_set):
        cols_match = True
        ValidationLogger.info("Data columns match schema.")
    else:
        cols_match = False
        ValidationLogger.warning("Data columns do not match schema.")
        missing_file_cols = (
            f"Missing from dataframe: {schema_full_col_set - df_cols_set}"
        )
        missing_df_cols = f"Missing from schema: {df_cols_set - schema_full_col_set}"
        ValidationLogger.warning(missing_file_cols)
        ValidationLogger.warning(missing_df_cols)
        if raise_error:
            raise ValueError("Error: The the number of columns do not match. Halted")

    ValidationLogger.info(
        f"Length of data: {len(df_cols_set)}. Length of schema: "
        f"{len(schema_full_col_set)}"
    )

    return cols_match


def validate_data_with_schema(survey_df: pd.DataFrame, schema_path: str):  # noqa: C901
    """Takes the schema from the toml file and validates the survey data df.

    Args:
        survey_df (pd.DataFrame): Survey data in a pd.df format
        schema_path (str): path to the schema toml (should be in config folder)
    """
    ValidationLogger.info(f"Starting validation with {schema_path}")
    # Load schema from toml
    dtypes_schema = load_schema(schema_path)

    if not dtypes_schema:
        raise FileNotFoundError(f"File at {schema_path} does not exist. Check path")

    # Create a dict for dtypes only
    dtypes_dict = {
        column_nm: dtypes_schema[column_nm]["Deduced_Data_Type"]
        for column_nm in dtypes_schema.keys()
    }

    # Cast each column individually and catch any errors
    for column in dtypes_dict.keys():
        # Fix for the columns which contain empty strings. We want to cast as NaN
        if dtypes_dict[column] == "pd.NA":
            # Replace whatever is in that column with np.nan
            survey_df[column] = np.nan
            dtypes_dict[column] = "float64"

        try:
            if dtypes_dict[column] == "Int64":
                # Convert non-integer string to NaN
                survey_df[column] = survey_df[column].apply(
                    pd.to_numeric, errors="coerce"
                )
                # Cast columns to Int64
                survey_df[column] = survey_df[column].astype(pd.Int64Dtype())
            elif dtypes_dict[column] == "str":
                survey_df[column] = survey_df[column].astype("string")
            elif "datetime" in dtypes_dict[column]:
                try:
                    survey_df[column] = pd.to_datetime(
                        survey_df[column], errors="coerce", dayfirst=True
                    )
                except TypeError:
                    raise TypeError(
                        f"Failed to convert column '{column}' to datetime. Please check"
                        " the data."
                    )
            else:
                survey_df[column] = survey_df[column].astype(dtypes_dict[column])
        except Exception as e:
            ValidationLogger.error(e)
    ValidationLogger.info("Validation successful")


@time_logger_wrap
@exception_wrap
def combine_schemas_validate_full_df(
    survey_df: pd.DataFrame, contributor_schema: "str", wide_response_schema: "str"
):
    """Takes the schemas from the toml file and validates the survey data df.

    Args:
        survey_df (pd.DataFrame): Survey data in a pd.df format
        contributor_schema (str): path to the schema toml (should be in config folder)
        wide_response_schema (str): path to the schema toml (should be in config folder)
    """

    # Load schemas from toml
    ValidationLogger.info("Loading contributer and wide schemas from toml")
    dtypes_con_schema = load_schema(contributor_schema)
    dtypes_res_schema = load_schema(wide_response_schema)

    # Create all unique keys from both schema
    full_columns_list = set(dtypes_con_schema) | set(dtypes_res_schema)

    # Create a dict for dtypes only
    dtypes = {
        column_nm: dtypes_con_schema[column_nm]["Deduced_Data_Type"]
        if column_nm in dtypes_con_schema
        else dtypes_res_schema[column_nm]["Deduced_Data_Type"]
        for column_nm in full_columns_list
    }

    # Cast each column individually and catch any errors
    ValidationLogger.info("Starting data type casting process")
    for column in survey_df.columns:
        # Fix for the columns which contain empty strings. We want to cast as NaN
        if dtypes[column] == "pd.NA":
            # Replace whatever is in that column with np.nan
            survey_df[column] = np.nan
            dtypes[column] = "float64"

            # Try to cast each column to the required data type

        if dtypes[column] == "Int64":
            # Convert non-integer string to NaN
            survey_df[column] = survey_df[column].apply(pd.to_numeric, errors="coerce")
            # Cast columns to Int64
            survey_df[column] = survey_df[column].astype("Int64")
        elif dtypes[column] == "float64":
            # Convert non-integer string to NaN
            survey_df[column] = survey_df[column].apply(pd.to_numeric, errors="coerce")
            # Cast columns to float64
            survey_df[column] = survey_df[column].astype("float64", errors="ignore")
        elif dtypes[column] == "str":
            survey_df[column] = survey_df[column].astype("string")
        else:
            survey_df[column] = survey_df[column].astype(dtypes[column])
    ValidationLogger.info("Finished data type casting process")


@time_logger_wrap
@exception_wrap
def validate_many_to_one(*args) -> pd.DataFrame:
    """
    Validates a many-to-one mapper DataFrame.

    This function performs the following checks:
    1. Checks if the mapper has two specified columns, referred to as 'col_many' and
        'col_one'.
    2. Selects and deduplicates 'col_many' and 'col_one'.
    3. Checks that for each entry in 'col_many' there is exactly one corresponding
        entry in 'col_one'.

    Args:
        *args: Variable length argument list. It should contain the following items
            in order:
            - df (pd.DataFrame): The input mapper DataFrame.
            - col_many (str): The name of the column with many entries.
            - col_one (str): The name of the column with one entry.

    Returns:
        pd.DataFrame: The validated mapper DataFrame with deduplicated 'col_many' and
            'col_one' columns.

    Raises:
        ValueError: If the mapper does not have the 'col_many' and 'col_one' columns,
            or if there are multiple entries in 'col_one' for any entry in 'col_many'.
    """

    mapper = args[0]
    col_many = args[1]
    col_one = args[2]
    try:
        # Check that expected column are present
        cols = mapper.columns
        if not ((col_many in cols) and (col_one in cols)):
            raise ValueError(f"Mapper must have columns {col_many} and {col_one}")

        # Selects the columns we need and deduplicates
        df = mapper[[col_many, col_one]].drop_duplicates()

        # Check the count of col_one
        df_count = (
            df.groupby(col_many)
            .agg({col_one: "count"})
            .reset_index()
            .rename(columns={col_one: "code_count"})
        )
        df_bad = df_count[df_count["code_count"] > 1]
        if not df_bad.empty:
            ValidationLogger.info(
                "The following codes have multile mapping: \n {df_bad}"
            )
            raise ValueError("Mapper is many to many")
        return df

    except ValueError as ve:
        raise ValueError("Many-to-one mapper validation failed: " + str(ve))


@exception_wrap
def validate_cora_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates cora mapper df:
    1. Checks if the DataFrame has exactly two columns.
    2. Checks if the column headers are "statusencoded" and "form_status".
    3. Checks the validity of the columns contents.
    Args:
        df (pd.DataFrame): The input DataFrame containing "statusencoded"
        and "form_status" columns.
    Returns:
        df (pd.DataFrame): with cols changed to string type
    """
    try:
        # Check if the dataframe has exactly two columns
        if df.shape[1] != 2:
            raise ValueError("DataFrame must have exactly two columns")

        # Check if the column headers are "statusencoded" and "form_status"
        if list(df.columns) != ["statusencoded", "form_status"]:
            raise ValueError("Column headers must be 'statusencoded' and 'form_status'")

        # Check the contents of the "status" and "form_status" columns
        status_check = df["statusencoded"].astype("str").str.len() == 3
        from_status_check = df["form_status"].astype("str").str.len().isin([3, 4])

        # Create the "contents_check" column based on the checks
        df["contents_check"] = status_check & from_status_check

        # Check if there are any False values in the "contents_check" column
        if (df["contents_check"] == False).any():  # noqa
            raise ValueError("Unexpected format within column contents")

        # Drop the "contents_check" column
        df.drop(columns=["contents_check"], inplace=True)

        return df

    except ValueError as ve:
        raise ValueError("cora status mapper validation failed: " + str(ve))


def flag_no_rand_spenders(df, raise_or_warn):
    """
    Flags any records that answer "No" to "604" and also report their expenditure in
    "211" as more than 0.

    Args:
        df (pandas.DataFrame): The input DataFrame.

    Returns:
        None
    """
    invalid_records = df.loc[(df["604"] == "No") & (df["211"] > 0)]

    if not invalid_records.empty:
        if raise_or_warn == "raise":
            raise Exception("Some records report no R&D, but spend in 211 > 0.")
        elif raise_or_warn == "warn":
            total_invalid_spend = invalid_records["211"].sum()
            ValidationLogger.error("Some records report no R&D, but spend in 211 > 0.")
            ValidationLogger.error(
                f"The total spend of 'No' R&D companies is £{int(total_invalid_spend)}"
            )
            ValidationLogger.error(invalid_records)

    else:
        ValidationLogger.info("All records have valid R&D spend.")
