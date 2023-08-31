import os
import toml
import postcodes_uk
import pandas as pd
from numpy import nan

import logging
from src.utils.wrappers import time_logger_wrap, exception_wrap

# from src.utils.helpers import Config_settings

# Get the config
# conf_obj = Config_settings(config_path)
# config = conf_obj.config_dict
# global_config = config["global"]

# Set up logging
validation_logger = logging.getLogger(__name__)


def validate_postcode_pattern(pcode: str) -> bool:
    """A function to validate UK postcodes which uses the postcodes_uk package
    to verify the pattern of a postcode by using regex.

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


@time_logger_wrap
@exception_wrap
def validate_post_col(
    df: pd.DataFrame, postcode_masterlist: pd.DataFrame, config: dict
):
    """This function checks if all postcodes in the specified DataFrame column
        are valid UK postcodes. It uses the `validate_postcode_pattern` function to
        perform the validation.

    Args:
        df (pd.DataFrame): The DataFrame containing the postcodes.
        postcode_masterlist (pd.DataFrame): The dataframe containing the correct
        postocdes to check against

    Returns:
        invalid_df (pd.DataFrame): A dataframe of postcodes with the incorrect pattern
        unreal_df (pd.DataFrame): A dataframe of postcodes not found in the masterlist
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"The dataframe you are attempting to validate is {type(df)}")

    # Check if postcodes match pattern
    # Create list of postcodes with incorrect patterns
    invalid_pattern_postcodes = df.loc[
        ~df["referencepostcode"].apply(validate_postcode_pattern), "referencepostcode"
    ]

    # Save to df
    invalid_df = pd.DataFrame(
        {
            "reference": df.loc[invalid_pattern_postcodes.index, "reference"],
            "instance": df.loc[invalid_pattern_postcodes.index, "instance"],
            "invalid_pattern_pcodes": invalid_pattern_postcodes,
        }
    )

    # Log the invalid postcodes
    if not invalid_pattern_postcodes.empty:
        validation_logger.warning(
            f"Invalid pattern postcodes found: {invalid_pattern_postcodes.to_list()}"
        )

    # Create a list of postcodes not found in masterlist
    unreal_postcodes = check_pcs_real(df, postcode_masterlist, config)

    # Save to df
    unreal_df = pd.DataFrame(
        {
            "reference": df.loc[unreal_postcodes.index, "reference"],
            "instance": df.loc[unreal_postcodes.index, "instance"],
            "not_real_pcodes": unreal_postcodes,
        }
    )

    # Log the unreal postcodes
    if not unreal_postcodes.empty:
        validation_logger.warning(
            f"These postcodes are not found in the ONS postcode list: {unreal_postcodes.to_list()}"  # noqa
        )

    # Combine the two lists
    combined_invalid_postcodes = pd.concat(
        [unreal_postcodes, invalid_pattern_postcodes]
    )
    combined_invalid_postcodes.drop_duplicates(inplace=True)

    if not combined_invalid_postcodes.empty:
        validation_logger.warning(
            f"Total list of unique invalid postcodes found: {combined_invalid_postcodes.to_list()}"  # noqa
        )

        validation_logger.warning(
            f"Total count of unique invalid postcodes found: {len(combined_invalid_postcodes.to_list())}"  # noqa
        )

    validation_logger.info("All postcodes validated....")

    return invalid_df, unreal_df


def insert_space(postcode, position):
    return postcode[0:position] + " " + postcode[position:]


@exception_wrap
def get_masterlist(postcode_masterlist) -> pd.Series:
    """This function converts the masterlist dataframe to a Pandas series

    Returns:
        pd.Series: A series of postcodes
    """
    masterlist = postcode_masterlist.squeeze()

    return masterlist


def check_pcs_real(df: pd.DataFrame, postcode_masterlist: pd.DataFrame, config: dict):
    """Checks if the postcodes are real against a masterlist of actual postcodes.

    In the masterlist, all postcodes are 7 characters long, therefore the
    reference are formatted to match this format.

    All postcodes above 7 characters are stripped of whitespaces.
    All postcodes less than 7 characters have added whitespaces in the middle.

    This formatting is applied to a copy dataframe so the original is unchanged.

    The final output are the postcodes from the original dataframe

    Args:
        df (pd.DataFrame): The DataFrame containing the postcodes.
        postcode_masterlist (pd.DataFrame): The dataframe containing the correct
        postocdes to check against

    Returns:
        unreal_postcodes (pd.DataFrame): A dataframe containing all the
        original postcodes not found in the masterlist

    """
    # Create a copy df for validation
    check_real_df = df.copy()

    if config["global"]["postcode_csv_check"]:
        master_series = get_masterlist(postcode_masterlist)

        # Renove whitespaces on larger than 7 characters
        check_real_df.loc[
            check_real_df["referencepostcode"].str.len() > 7, "referencepostcode"
        ] = check_real_df.loc[
            check_real_df["referencepostcode"].str.len() > 7, "referencepostcode"
        ].str.replace(
            " ", ""
        )

        # Add whitespace to short postcodes
        check_real_df.loc[
            df["referencepostcode"].str.len() < 7, "referencepostcode"
        ] = check_real_df.loc[
            check_real_df["referencepostcode"].str.len() < 7, "referencepostcode"
        ].apply(
            lambda x: insert_space(x, 3)
        )

        # Check if postcode are real
        unreal_postcodes = df.loc[
            ~check_real_df["referencepostcode"].isin(master_series), "referencepostcode"
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
    contributor_schema: str = "./config/contributors_schema.toml",
    wide_respon_schema: str = "./config/wide_responses.toml",
) -> bool:
    """Compares the shape of the data and compares it to the shape of the toml
    file based off the data schema. Returns true if there is a match and false
    otherwise.

    Keyword Arguments:
        data_df(pd.DataFrame): Pandas dataframe containing data to be checked.
        contributor_schema(str): Path to the schema toml (should be in config folder)
        wide_respon_schema(str): Path to the schema toml (should be in config folder)
    Returns:
        A bool: boolean, True if number of columns is as expected, otherwise False
    """
    if not isinstance(data_df, pd.DataFrame):
        raise ValueError(
            f"data_df must be a pandas dataframe, is currently {type(data_df)}."
        )

    cols_match = False

    data_dict = data_df.to_dict()

    # Load toml data schemas into dictionary
    toml_string_cont = load_schema(contributor_schema)
    toml_string_response = load_schema(wide_respon_schema)

    # Combained two dicts
    full_columns_list = {**toml_string_cont, **toml_string_response}

    # Filtered schema colum if is in data columns
    toml_string = [key for key in full_columns_list.keys() if key in data_df.columns]

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
    for column in dtypes_dict.keys():

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
        validation_logger.debug(f"{column} after: {survey_df[column].dtype}")
