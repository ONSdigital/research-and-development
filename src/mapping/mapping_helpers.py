import pandas as pd
import re
import numpy as np
import logging
from typing import Callable
import os
import toml

from src.utils.wrappers import time_logger_wrap, exception_wrap

MappingLogger = logging.getLogger(__name__)

def load_validate_mapper(
    mapper_path_key,
    paths,
    file_exists_func,
    read_csv_func,
    logger,
    val_with_schema_func: Callable,
    one_to_many_val_func: Callable,
    *args,
):
    """
    Loads a specified mapper, validates it using a schema and an optional
    validation function.

    This function first retrieves the path of the mapper from the provided paths
    dictionary using the mapper_path_key. It then checks if the file exists at
    the mapper path. If the file exists, it is read into a DataFrame. The
    DataFrame is then validated against a schema, which is located at a path
    constructed from the mapper name. If a validation function is provided, it
    is called with the DataFrame and any additional arguments.

    Args:
        mapper_path_key (str): The key to retrieve the mapper path from the
        paths dictionary.

        paths (dict): A dictionary containing paths.
        file_exists_func (Callable): A function to check if a file exists at a
        given path.
        read_csv_func (Callable): A function to read a CSV file into a
        DataFrame.
        logger (logging.Logger): A logger to log information and errors.
        val_with_schema_func (Callable): A function to validate a DataFrame
        against a schema.
        validation_func (Callable, optional): An optional function to perform
        additional validation on the DataFrame.
        *args: Additional arguments to pass to the validation function.

    Returns:
        pd.DataFrame: The loaded and validated mapper DataFrame.

    Raises:
        FileNotFoundError: If no file exists at the mapper path.
        ValidationError: If the DataFrame fails schema validation or the validation func
    """
    # Get the path of the mapper from the paths dictionary
    mapper_path = paths[mapper_path_key]

    # Get the name of the mapper from the mapper path key
    mapper_name = getmappername(mapper_path_key, split=True)

    # Log the loading of the mapper
    logger.info(f"Loading {getmappername(mapper_path_key, split=True)} to File...")

    # Check if the file exists at the mapper path, raise an error if it doesn't
    file_exists_func(mapper_path, raise_error=True)

    # Read the file at the mapper path into a DataFrame
    mapper_df = read_csv_func(mapper_path)

    # Construct the path of the schema from the mapper name
    schema_prefix = "_".join(word for word in mapper_name.split() if word != "mapper")
    schema_path = f"./config/{schema_prefix}_schema.toml"

    # Validate the DataFrame against the schema
    val_with_schema_func(mapper_df, schema_path)

    # If a one-to-many validation function is provided, validate the DataFrame
    if one_to_many_val_func:
        # Prepend the DataFrame to the arguments
        args = (mapper_df,) + args
        # Call the validation function with the DataFrame and the other arguments
        one_to_many_val_func(*args)  # args include "col_many" and "col_one"

    # Log the successful loading of the mapper
    logger.info(f"{mapper_name} loaded successfully")

    # Return the loaded and validated DataFrame
    return mapper_df


def getmappername(mapper_path_key, split):
    """
    Extracts the mapper name from a given path key.

    This function uses a regular expression to extract the name of the mapper from the
    provided key.
    The name is assumed to be the part of the key before the first underscore.
    If the 'split' parameter is True, underscores in the name are replaced with spaces.

    Parameters:
    mapper_path_key (str): The key from which to extract the mapper name.
    split (bool): Whether to replace underscores in the name with spaces.

    Returns:
    str: The extracted mapper name.
    """
    patt = re.compile(r"^(.*?)_path")
    mapper_name = re.search(patt, mapper_path_key).group(1)

    if split:
        mapper_name = mapper_name.replace("_", " ")

    return mapper_name


@time_logger_wrap
def validate_data_with_schema(survey_df: pd.DataFrame, schema_path: str):
    """Takes the schema from the toml file and validates the survey data df.

    Args:
        survey_df (pd.DataFrame): Survey data in a pd.df format
        schema_path (str): path to the schema toml (should be in config folder)
    """
    MappingLogger.info(f"Starting validation with {schema_path}")
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
            MappingLogger.debug(f"{column} before: {survey_df[column].dtype}")
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
                        survey_df[column], errors="coerce"
                    )
                except TypeError:
                    raise TypeError(
                        f"Failed to convert column '{column}' to datetime. Please check the data."
                    )
            else:
                survey_df[column] = survey_df[column].astype(dtypes_dict[column])
            MappingLogger.debug(f"{column} after: {survey_df[column].dtype}")
        except Exception as e:
            MappingLogger.error(e)
    MappingLogger.info("Validation successful")


@time_logger_wrap
@exception_wrap
def validate_many_to_one(*args) -> pd.DataFrame:
    """
    Validates a many-to-one mapper DataFrame.

    This function performs the following checks:
    1. Checks if the mapper has two specified columns, referred to as 'col_many' and 'col_one'.
    2. Selects and deduplicates 'col_many' and 'col_one'.
    3. Checks that for each entry in 'col_many' there is exactly one corresponding entry in 'col_one'.

    Args:
        *args: Variable length argument list. It should contain the following items in order:
            - df (pd.DataFrame): The input mapper DataFrame.
            - col_many (str): The name of the column with many entries.
            - col_one (str): The name of the column with one entry.

    Returns:
        pd.DataFrame: The validated mapper DataFrame with deduplicated 'col_many' and 'col_one' columns.

    Raises:
        ValueError: If the mapper does not have the 'col_many' and 'col_one' columns, or if there are multiple entries in 'col_one' for any entry in 'col_many'.
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
            MappingLogger.info(
                "The following codes have multile mapping: \n {df_bad}"
            )
            raise ValueError("Mapper is many to many")
        return df

    except ValueError as ve:
        raise ValueError("Many-to-one mapper validation failed: " + str(ve))


@time_logger_wrap
@exception_wrap
def validate_ultfoc_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates ultfoc df:
    1. Checks if the DataFrame has exactly two columns.
    2. Checks if the column headers are 'ruref' and 'ultfoc'.
    3. Checks the validity of values in the 'ultfoc' column.
    Args:
        df (pd.DataFrame): The input DataFrame containing 'ruref'
        and 'ultfoc' columns.
    """
    try:
        # Check DataFrame shape
        if df.shape[1] != 2:
            raise ValueError("Dataframe file must have exactly two columns")

        # Check column headers
        if list(df.columns) != ["ruref", "ultfoc"]:
            raise ValueError("Column headers should be 'ruref' and 'ultfoc'")

        # Check 'ultfoc' values are either 2 characters or 'nan'
        def check_ultfoc(value):
            if pd.isna(value):
                return True
            else:
                return isinstance(value, str) and (len(value) == 2)

        df["contents_check"] = df.apply(lambda row: check_ultfoc(row["ultfoc"]), axis=1)

        # check any unexpected contents
        if not df["contents_check"].all():
            raise ValueError("Unexpected format within 'ultfoc' column contents")

        df.drop(columns=["contents_check"], inplace=True)

    except ValueError as ve:
        raise ValueError("Foreign ownership mapper validation failed: " + str(ve))


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
        MappingLogger.warning(
            "Validation schema does not exist! Path may be incorrect"
        )
        return file_exists

    return toml_dict


def update_ref_list(full_df: pd.DataFrame, ref_list_df: pd.DataFrame) -> pd.DataFrame:
    """
    Update long form references that should be on the reference list.

    For the first year (processing 2022 data) only, several references
    should have been designated on the "reference list", ie, should have been
    assigned cellnumber = 817, but were wrongly assigned a different cellnumber.

    Args:
        full_df (pd.DataFrame): The full_responses dataframe
        ref_list_df (pd.DataFrame): The mapper containing updates for the cellnumber
    Returns:
        df (pd.DataFrame): with cellnumber and selectiontype cols updated.
    """
    ref_list_filtered = ref_list_df.loc[
        (ref_list_df.formtype == "1") & (ref_list_df.cellnumber != 817)
    ]
    df = pd.merge(
        full_df,
        ref_list_filtered[["reference", "cellnumber"]],
        how="outer",
        on="reference",
        suffixes=("", "_new"),
        indicator=True,
    )
    # check no items in the reference list mapper are missing from the full responses
    missing_refs = df.loc[df["_merge"] == "right_only"]
    if not missing_refs.empty:
        msg = (
            "The following references in the reference list mapper are not in the data:"
        )
        raise ValueError(msg + str(missing_refs.reference.unique()))

    # update cellnumber and selectiontype where there is a match
    match_cond = df["_merge"] == "both"
    df = df.copy()
    df.loc[match_cond, "cellnumber"] = 817
    df.loc[match_cond, "selectiontype"] = "L"

    df = df.drop(["_merge", "cellnumber_new"], axis=1)

    return df
