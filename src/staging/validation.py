import os
import tomli
import pandas as pd
import numpy as np

import logging

# Set up logging
ValidationLogger = logging.getLogger(__name__)


def load_schema(file_path: str) -> dict:
    """Load the data schema from toml file into a dictionary

    Keyword Arguments:
        file_path -- Path to data schema toml file
        (default: {"./config/contributors_schema.toml"})

    Raises:
        FileNotFoundError: If the file does not exist

    Returns:
        A dict: dictionary containing parsed schema toml file
    """
    # Create bool variable for checking if file exists
    file_exists = os.path.exists(file_path)

    # Check if Data_Schema.toml exists
    if file_exists:
        try:
            # Open the file and load toml data schema into dictionary
            with open(file_path, "rb") as file:
                toml_dict = tomli.load(file)
            return toml_dict
        except tomli.TOMLDecodeError as e:
            ValidationLogger.error(f"Error decoding TOML file at {file_path}: {e}")
            raise
    else:
        raise FileNotFoundError(f"File at {file_path} does not exist. Check path")


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


def validate_bool_cols(bool_column: pd.Series, nullable: bool = True) -> pd.Series:
    """
    Validates a boolean column in a DataFrame.

    If `nullable` is False, any null values in the column are filled with False and
    the datatype is set to `bool` (which does not allow nulls).
    If `nullable` is True, the datatype is set to pandas' nullable `boolean` type.

    Args:
        bool_column (pd.Series): The boolean column to validate.
        nullable (bool): Whether the column is allowed to have null values.

    Returns:
        pd.Series: The validated boolean column.
    """
    bool_mapping = {
        "True": True,
        "False": False,
        "TRUE": True,
        "FALSE": False,
        "true": True,
        "false": False,
    }
    # Map the values in the boolean column to their corresponding boolean values
    validated_column = bool_column.astype("string").map(bool_mapping)
    if not nullable:
        validated_column = validated_column.fillna(False).astype(bool)
    else:
        validated_column = validated_column.astype("boolean")

    return validated_column


def _process_numeric_cols(df_column: pd.Series, designated_dtype: str) -> pd.Series:
    """Helper function to process numeric columns.

    Args:
        df_column (pd.Series): DataFrame column to be processed

    Returns:
        pd.Series: Processed DataFrame column
    """
    # Convert non-numeric strings to nan
    df_column = df_column.apply(pd.to_numeric, errors="coerce")

    # we no longer want to use "Int64" in the pipeline as it causes many probs
    if designated_dtype in ["Int64", "int64", "int"]:
        # see if there are any nulls in the column, if so convert to float
        if df_column.isnull().any():
            df_column = df_column.astype("float64")
        # otherwise cast as int64 (small i)
        else:
            df_column = df_column.astype("int64")
    else:
        df_column = df_column.astype("float64")

    return df_column


def _process_datetime_cols(df_column: pd.Series) -> pd.Series:
    """Helper function to process datetime columns.

    Args:
        df_column (pd.Series): DataFrame column to be processed

    Raises:
        TypeError: If the column cannot be converted to datetime

    Returns:
        pd.Series: Processed DataFrame column
    """
    try:
        df_column = pd.to_datetime(df_column, errors="coerce", dayfirst=True)
    except TypeError:
        e = f"Failed to convert column '{df_column.name}' to datetime. "
        raise TypeError(e)
    return df_column


def _validate_bool_cols(bool_column: pd.Series, nullable: bool = True) -> pd.Series:
    """
    Validates a boolean column in a DataFrame.
    If `nullable` is False, any null values in the column are filled with False and
    the datatype is set to `bool` (which does not allow nulls).
    If `nullable` is True, the datatype is set to pandas' nullable `boolean` type.
    Args:
        bool_column (pd.Series): The boolean column to validate.
        nullable (bool): Whether the column is allowed to have null values.
    Returns:
        pd.Series: The validated boolean column.
    """
    bool_mapping = {
        "True": True,
        "False": False,
        "TRUE": True,
        "FALSE": False,
        "true": True,
        "false": False,
    }
    # Map the values in the boolean column to their corresponding boolean values
    validated_column = bool_column.astype("string").map(bool_mapping)
    if not nullable:
        validated_column = validated_column.fillna(False).astype(bool)
    else:
        validated_column = validated_column.astype("boolean")

    return validated_column


def process_data_types(df_column: pd.Series, designated_dtype: str) -> pd.Series:
    """Casts each column in the dataframe to the data type specified in the
    dtype_dict.

    Args:
        df_column (pd.Series): DataFrame to be processed
        designated_dtype (str): Designated data type for the column

    Raises:
        TypeError: If the designated data type is not recognized

    Returns:
        pd.Series: Processed DataFrame column with correct data type
    """
    ValidationLogger.debug(
        f"Validating col '{df_column.name}' with designated dtype '{designated_dtype}'"
    )
    try:
        # numeric
        if designated_dtype in ["Int64", "int64", "int", "float64", "float"]:
            df_column = _process_numeric_cols(df_column, designated_dtype)
        # strings
        elif designated_dtype in ["str", "string", "object"]:
            # use the pandas string type for better performance
            # and to avoid issues with mixed types
            df_column = df_column.astype("string")
        # booleans
        elif designated_dtype in ["bool", "boolean"]:
            nullable = True if designated_dtype == "boolean" else False
            df_column = _validate_bool_cols(df_column, nullable=nullable)
        # datetimes
        elif "datetime" in designated_dtype:
            df_column = _process_datetime_cols(df_column)
        else:
            e = f"Designated data type '{designated_dtype}' for column "
            e += f"'{df_column.name}' is not recognized."
            raise TypeError(e)
    except Exception as e:
        ValidationLogger.error(f"{df_column.name}: {e}")
    return df_column


def validate_data_with_schema(
    survey_df: pd.DataFrame, schema_path: str, warn_or_raise: str = "warn"
) -> pd.DataFrame:
    """Takes the schema from the toml file and validates the survey data df.

    Args:
        survey_df (pd.DataFrame): Survey data in a pd.df format
        schema_path (str): path to the schema toml (should be in config folder)
        warn_or_raise (str): Whether to 'warn' or 'raise' an error if a column
            from the schema is missing in the dataframe. Defaults to 'warn'.

    Raises:
        KeyError: If a column from the schema is missing in the dataframe and
            warn_or_raise is set to 'raise'.

    Returns:
        pd.DataFrame: DataFrame with validated data types
    """
    dtypes_schema = load_schema(schema_path)
    dtypes_dict = {
        column_nm: dtypes_schema[column_nm]["Deduced_Data_Type"]
        for column_nm in dtypes_schema.keys()
    }

    # Cast each column individually, and catch any errors
    for column in dtypes_dict.keys():
        # Check whether the column is in the dataframe
        if column not in survey_df.columns:
            if warn_or_raise == "raise":
                raise KeyError(f"Column '{column}' is not present in the DataFrame.")
            else:
                ValidationLogger.warning(
                    f"Column '{column}' is not present in the DataFrame. Skipping."
                )
                continue

        # ensure consistent handling of nulls
        survey_df[column] = survey_df[column].replace(
            [pd.NA, "", " ", None, "<blank>", "N/A", "NA", "<NA>"], np.nan
        )

        designated_dtype = dtypes_dict[column]
        survey_df[column] = process_data_types(survey_df[column], designated_dtype)

    ValidationLogger.info("Validation successful")
    return survey_df


def combine_schemas_validate_full_df(
    survey_df: pd.DataFrame,
    contributor_schema: "str",
    wide_response_schema: "str",
    warn_or_raise: str = "raise",
) -> pd.DataFrame:
    """Takes the schemas from the toml file and validates the survey data df.

    The survey dataframe is been created by joining contributor and wide response data.
    Therefore, the two schemas are combined to create a full schema for validation.

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

    # Create dtypes dictionary for the full schema, both contributor and wide
    dtypes_dict = {
        column_nm: (
            dtypes_con_schema[column_nm]["Deduced_Data_Type"]
            if column_nm in dtypes_con_schema
            else dtypes_res_schema[column_nm]["Deduced_Data_Type"]
        )
        for column_nm in full_columns_list
    }
    # Cast each column individually, and catch any errors
    for column in dtypes_dict.keys():
        # Check whether the column is in the dataframe
        if column not in survey_df.columns:
            if warn_or_raise == "raise":
                raise KeyError(f"Column '{column}' is not present in the DataFrame.")
            else:
                ValidationLogger.warning(
                    f"Column '{column}' is not present in the DataFrame. Skipping."
                )
                continue

        # ensure consistent handling of nulls
        survey_df[column] = survey_df[column].replace(
            [pd.NA, "", " ", None, "<blank>", "N/A", "NA", "<NA>"], np.nan
        )
        # process the data types
        designated_dtype = dtypes_dict[column]
        survey_df[column] = process_data_types(survey_df[column], designated_dtype)

    ValidationLogger.info("Validation successful")
    return survey_df


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
