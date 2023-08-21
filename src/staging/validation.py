import os
import toml
import postcodes_uk
import pandas as pd
from numpy import nan

import logging
from src.utils.wrappers import time_logger_wrap, exception_wrap
from src.utils.helpers import Config_settings

# Get the config
conf_obj = Config_settings()
config = conf_obj.config_dict
global_config = config["global"]

# Set up logging
validation_logger = logging.getLogger(__name__)


def validate_postcode_pattern(pcode: str) -> bool:
    """A function to validate UK postcodes which uses the

    Args:
        pcode (str): The postcode to validate

    Returns:
        bool: True or False depending on if it is valid or not
    """
    if pcode is None:
        return False

    # Validation step
    valid_bool = postcodes_uk.validate(pcode)

    return valid_bool


@exception_wrap
def get_masterlist(postcode_masterlist) -> pd.Series:
    """This function loads the masterlist of postcodes from a csv file

    Returns:
        pd.Series: The dataframe of postcodes
    """
    masterlist = pd.read_csv(postcode_masterlist, usecols=["pcd"]).squeeze()
    return masterlist


@time_logger_wrap
@exception_wrap
def validate_post_col(df: pd.DataFrame, postcode_masterlist: str) -> bool:
    """This function checks if all postcodes in the specified DataFrame column
        are valid UK postcodes. It uses the `validate_postcode` function to
        perform the validation.

    Args:
        df (pd.DataFrame): The DataFrame containing the postcodes.

    Returns:
        A bool: boolean, True if number of columns is as expected, otherwise False
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"The dataframe you are attempting to validate is {type(df)}")

    unreal_postcodes = check_pcs_real(df, postcode_masterlist)

    # Log the unreal postcodes
    if not unreal_postcodes.empty:
        validation_logger.warning(
            f"These postcodes are not found in the ONS postcode list: {unreal_postcodes.to_list()}"  # noqa
        )

    # Check if postcodes match pattern
    invalid_pattern_postcodes = df.loc[
        ~df["referencepostcode"].apply(validate_postcode_pattern), "referencepostcode"
    ]

    # Log the invalid postcodes
    if not invalid_pattern_postcodes.empty:
        validation_logger.warning(
            f"Invalid pattern postcodes found: {invalid_pattern_postcodes.to_list()}"
        )

    # Combine the two lists
    combined_invalid_postcodes = pd.concat(
        [unreal_postcodes, invalid_pattern_postcodes]
    )
    combined_invalid_postcodes.drop_duplicates(inplace=True)

    if not combined_invalid_postcodes.empty:
        raise ValueError(
            f"Invalid postcodes found: {combined_invalid_postcodes.to_list()}"
        )

    validation_logger.info("All postcodes validated....")

    return True


def check_pcs_real(df: pd.DataFrame, postcode_masterlist: str):
    """Checks if the postcodes are real against a masterlist of actual postcodes"""
    if config["global"]["postcode_csv_check"]:
        master_series = get_masterlist(postcode_masterlist)

        # Check if postcode are real
        unreal_postcodes = df.loc[
            ~df["referencepostcode"].isin(master_series), "referencepostcode"
        ]
    else:
        emptydf = pd.DataFrame(columns=["referencepostcode"])
        unreal_postcodes = emptydf.loc[
            ~emptydf["referencepostcode"], "referencepostcode"
        ]

    return unreal_postcodes


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
        return file_exists

    return toml_dict


@exception_wrap
def check_data_shape(
    data_df: pd.DataFrame,
    schema_path: str = "./config/contributors_schema.toml",
) -> bool:
    """Compares the shape of the data and compares it to the shape of the toml
    file based off the data schema. Returns true if there is a match and false
    otherwise.

    Keyword Arguments:
        data_df -- Pandas dataframe containing data to be checked.
        schema_path -- Path to schema dictionary file
        (default: {"./config/DataSchema.toml"})

    Returns:
        A bool: boolean, True if number of columns is as expected, otherwise False
    """
    if not isinstance(data_df, pd.DataFrame):
        raise ValueError(
            f"data_df must be a pandas dataframe, is currently {type(data_df)}."
        )

    cols_match = False

    data_dict = data_df.to_dict()

    # Load toml data schema into dictionary
    toml_string = load_schema(schema_path)

    # Compare length of data dictionary to the data schema
    if len(data_dict) == len(toml_string):
        cols_match = True
    else:
        cols_match = False

    if cols_match is False:
        validation_logger.warning(f"Data columns match schema: {cols_match}.")
    else:
        validation_logger.info(f"Data columns match schema: {cols_match}.")

    validation_logger.info(
        f"Length of data: {len(data_dict)}. Length of schema: {len(toml_string)}"
    )
    return cols_match


def validate_data_with_schema(survey_df: pd.DataFrame, schema_path: str):
    """Takes the schema from the toml file and validates the survey data df.

    Args:
        survey_df (pd.DataFrame): Survey data in a pd.df format
        schema_path (str): path to the schema toml (should be in config folder)
    """

    # Load schema from toml
    dtypes_schema = load_schema(schema_path)

    # Create a dict for dtypes only
    dtypes_dict = {
        column_nm: dtypes_schema[column_nm]["Deduced_Data_Type"]
        for column_nm in dtypes_schema.keys()
    }

    # Cast each column individually and catch any errors
    for column in survey_df.columns:

        # Fix for the columns which contain empty strings. We want to cast as NaN
        if dtypes_dict[column] == "pd.NA":
            # Replace whatever is in that column with np.nan
            survey_df[column] = nan
            dtypes_dict[column] = "float64"

        try:
            validation_logger.debug(f"{column} before: {survey_df[column].dtype}")
            if dtypes_dict[column] == "Int64":
                # Convert non-integer string to NaN
                survey_df[column] = survey_df[column].apply(
                    pd.to_numeric, errors="coerce"
                )
                # Cast columns to Int64
                survey_df[column] = survey_df[column].astype(pd.Int64Dtype())
            else:
                survey_df[column] = survey_df[column].astype(dtypes_dict[column])
            validation_logger.debug(f"{column} after: {survey_df[column].dtype}")
        except Exception as e:
            validation_logger.error(e)


def validate_data_with_both_schema(
    survey_df: pd.DataFrame, schema_path1: "str", schema_path2: "str"
):
    """Takes the schemas from the toml file and validates the survey data df.

    Args:
        survey_df (pd.DataFrame): Survey data in a pd.df format
        schema_path1 (str): path to the schema toml (should be in config folder)
        schema_path2 (str): path to the schema toml (should be in config folder)
    """
    # Load schemas from toml
    dtypes_schema1 = load_schema(schema_path1)
    dtypes_schema2 = load_schema(schema_path2)

    # Create all unique keys from both schema
    full_columns_list = set(dtypes_schema1) | set(dtypes_schema2)

    # Create a dict for dtypes only
    dtypes = {
        column_nm: dtypes_schema1[column_nm]["Deduced_Data_Type"]
        if column_nm in dtypes_schema1
        else dtypes_schema2[column_nm]["Deduced_Data_Type"]
        for column_nm in full_columns_list
    }

    # Cast each column individually and catch any errors
    for column in survey_df.columns:
        # Fix for the columns which contain empty strings. We want to cast as NaN
        if dtypes[column] == "pd.NA":
            # Replace whatever is in that column with np.nan
            survey_df[column] = nan
            dtypes[column] = "float64"
        try:
            # Try to cast each column to the required data type
            validation_logger.debug(f"{column} before: {survey_df[column].dtype}")
            if dtypes[column] == "Int64":
                # Convert non-integer string to NaN
                survey_df[column] = survey_df[column].apply(
                    pd.to_numeric, errors="coerce"
                )
                # Cast columns to Int64
                survey_df[column] = survey_df[column].astype(pd.Int64Dtype())
            elif dtypes[column] == "float64":
                # Convert non-integer string to NaN
                survey_df[column] = survey_df[column].apply(
                    pd.to_numeric, errors="coerce"
                )
                # Cast columns to float64
                survey_df[column] = survey_df[column].astype("float64", errors="ignore")
            else:
                survey_df[column] = survey_df[column].astype(dtypes[column])
            validation_logger.debug(f"{column} after: {survey_df[column].dtype}")
        except Exception as e:
            validation_logger.error(e)


@exception_wrap
def cellno_unit_dict(cellno_df: pd.DataFrame) -> dict:
    """To creted dictioanry from The berd_2022_cellno_coverage.xlsx
    going to be use for mapping


    Args:
        cellno_df (pd.DataFrame):cellno coverage data frame

    Returns:
        dict: Dictionary contains cell_no as key, UNI_Count as values
    """

    # Filtering object columns then Convert to object columns as integer
    object_col = [col for col in cellno_df.columns if cellno_df[col].dtypes == "object"]
    for col in object_col:
        cellno_df[col] = cellno_df[col].str.replace(",", "").astype(int)

    # Creating dictionary cell_no and UNI_count
    cell_unit_dict = cellno_df.set_index("cell_no").to_dict()["UNI_Count"]

    return cell_unit_dict


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
    for column in survey_df.columns:
        # Fix for the columns which contain empty strings. We want to cast as NaN
        if dtypes[column] == "pd.NA":
            # Replace whatever is in that column with np.nan
            survey_df[column] = nan
            dtypes[column] = "float64"

            # Try to cast each column to the required data type
        validation_logger.debug(f"{column} before: {survey_df[column].dtype}")
        if dtypes[column] == "Int64":
            # Convert non-integer string to NaN
            survey_df[column] = survey_df[column].apply(pd.to_numeric, errors="coerce")
            # Cast columns to Int64
            survey_df[column] = survey_df[column].astype(pd.Int64Dtype())
        elif dtypes[column] == "float64":
            # Convert non-integer string to NaN
            survey_df[column] = survey_df[column].apply(pd.to_numeric, errors="coerce")
            # Cast columns to float64
            survey_df[column] = survey_df[column].astype("float64", errors="ignore")
        else:
            survey_df[column] = survey_df[column].astype(dtypes[column])
        validation_logger.debug(f"{column} after: {survey_df[column].dtype}")
