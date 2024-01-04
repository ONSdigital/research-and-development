import os
import toml
import postcodes_uk
import pandas as pd
import numpy as np

import logging
from src.utils.wrappers import time_logger_wrap, exception_wrap
from src.staging.staging_helpers import postcode_topup

# Set up logging
ValidationLogger = logging.getLogger(__name__)


class ColumnMismatch(Exception):
    def __init__(self, message=None):
        self.message = message
        super().__init__(message)


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

    pcode = pcode.upper().strip()

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

        - A `postcodes_harmonised` column is created using the value in col `601`,
        or the value in col `postcodereference` (IDBR) where col `601` is blank.
        - Validation checks run on `postcodes_harmonised`
        First we validate the pattern of the postcode.
        Secondly if a postcode is valid, we validate that the postcodes are real
        - One dataframe containing any invalid postcodes outputted with relevant
        record information (inc. postcode source).
        - Any invalid postcodes removed from the `postcodes_harmonised`
        column in dataframe.

    Args:
        df (pd.DataFrame): The DataFrame containing the postcodes.
        postcode_masterlist (pd.DataFrame): The dataframe containing the correct
        postocdes to check against
        config (dict): The postcode settings from the config settings

    Returns:
        combined_invalid_postcodes_df (pd.DataFrame): A dataframe of invalid postcodes,
        either with the incorrect pattern or not found in the masterlist
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"The dataframe you are attempting to validate is {type(df)}")

    # Create new column and fill with "601" and the nulls with "referencepostcode"
    df["postcodes_harmonised"] = df["601"].fillna(df["referencepostcode"])

    # Create a copy to work from and add temp "postcode_source" column
    validation_df = df.copy()
    validation_df["postcode_source"] = validation_df.apply(
        lambda row: "column '601'"
        if pd.notna(row["601"])
        else "column 'referencepostcode' (IDBR)",
        axis=1,
    )

    # Apply the pattern validation function
    invalid_pattern_postcodes = validation_df.loc[
        ~validation_df["postcodes_harmonised"].apply(validate_postcode_pattern),
        "postcodes_harmonised",
    ]

    # Save to df
    invalid_df = pd.DataFrame(
        {
            "reference": validation_df.loc[
                invalid_pattern_postcodes.index, "reference"
            ],
            "instance": validation_df.loc[invalid_pattern_postcodes.index, "instance"],
            "formtype": validation_df.loc[invalid_pattern_postcodes.index, "formtype"],
            "postcode_issue": "invalid pattern / format",
            "incorrect_postcode": invalid_pattern_postcodes,
            "postcode_source": validation_df.loc[
                invalid_pattern_postcodes.index, "postcode_source"
            ],
        }
    )

    # Log the invalid postcodes
    ValidationLogger.warning(
        f"Invalid pattern postcodes found: {invalid_pattern_postcodes.to_list()}"
    )
    ValidationLogger.warning(
        f"""Number of invalid pattern postcodes found:
        {len(invalid_pattern_postcodes.to_list())}"""
    )

    # Remove the invalid pattern postcodes before checking if they are real
    val_df = validation_df.loc[
        ~validation_df.index.isin(invalid_pattern_postcodes.index.to_list())
    ]

    # Clean postcodes to match the masterlist
    check_real_df = clean_postcodes(val_df, "postcodes_harmonised")

    # Only validate not null postcodes for the column "601"
    check_real_df = check_real_df.loc[~check_real_df["postcodes_harmonised"].isnull()]

    # Create a list of postcodes not found in masterlist in col "postcodes_harmonised"
    unreal_postcodes = check_pcs_real(
        val_df, check_real_df, postcode_masterlist, config
    )

    # Save to df
    unreal_df = pd.DataFrame(
        {
            "reference": validation_df.loc[unreal_postcodes.index, "reference"],
            "instance": validation_df.loc[unreal_postcodes.index, "instance"],
            "formtype": validation_df.loc[unreal_postcodes.index, "formtype"],
            "postcode_issue": "not found in masterlist",
            "incorrect_postcode": unreal_postcodes,
            "postcode_source": validation_df.loc[
                unreal_postcodes.index, "postcode_source"
            ],
        }
    )

    # Log the unreal postcodes
    ValidationLogger.warning(
        f"These postcodes are not found in the ONS postcode list: {unreal_postcodes.to_list()}"  # noqa
    )

    ValidationLogger.warning(
        f"Number of postcodes not found in the ONS postcode list: {len(unreal_postcodes.to_list())}"  # noqa
    )

    # Combine the two lists for logging
    combined_invalid_postcodes = pd.concat(
        [unreal_postcodes, invalid_pattern_postcodes]
    )
    combined_invalid_postcodes.drop_duplicates(inplace=True)

    if not combined_invalid_postcodes.empty:
        ValidationLogger.warning(
            f"Total list of unique invalid postcodes found: {combined_invalid_postcodes.to_list()}"  # noqa
        )

        ValidationLogger.warning(
            f"Total count of unique invalid postcodes found: {len(combined_invalid_postcodes.to_list())}"  # noqa
        )

    # Combine and sort the two dataframes for output
    combined_invalid_postcodes_df = pd.concat([invalid_df, unreal_df])

    combined_invalid_postcodes_df = combined_invalid_postcodes_df.sort_values(
        ["reference", "instance"], ascending=[True, True]
    ).reset_index(drop=True)

    # update df to exclude any invalid postcode entries
    df["postcodes_harmonised"] = df["postcodes_harmonised"].where(
        ~df["postcodes_harmonised"].isin(
            combined_invalid_postcodes_df["incorrect_postcode"]
        ),
        other=None,
    )

    df["postcodes_harmonised"] = df["postcodes_harmonised"].apply(postcode_topup)

    ValidationLogger.info("All postcodes validated....")

    return combined_invalid_postcodes_df


def insert_space(postcode, position):
    """Automate the insertion of a space in a string"""
    return postcode[0:position] + " " + postcode[position:]


@exception_wrap
def get_masterlist(postcode_masterlist) -> pd.Series:
    """This function converts the masterlist dataframe to a Pandas series

    Returns:
        pd.Series: A series of postcodes
    """
    masterlist = postcode_masterlist.squeeze()

    return masterlist


def clean_postcodes(df, column):
    """Clean the postcodes to fit the masterlist format
    This is done by applying all the following steps:

    - remove whitespaces on postcodes of 8+ characters
    - add 1 whitespace for 7 digit postcodes
    - add 2 whitespaces for 6 digit postcodes
    - add 3 whitespaces for 5 digit postcodes

    Once all the postcodes are 8 characters long, they match the pcd2 column
    we is used to validate postcodes.
    """

    # Create a copy df to manipulate
    check_real_df = df.copy()

    # Convert to uppercase and strip whitespaces at start/end
    check_real_df[column] = check_real_df[column].str.upper()
    check_real_df[column] = check_real_df[column].str.strip()

    # Renove whitespaces on larger than 8 characters
    check_real_df.loc[
        check_real_df[column].str.strip().str.len() > 8, column
    ] = check_real_df.loc[
        check_real_df[column].str.strip().str.len() > 8, column
    ].str.replace(
        " ", ""
    )

    # Add whitespace to 7 digit postcodes
    check_real_df.loc[
        check_real_df[column].str.strip().str.len() == 7, column
    ] = check_real_df.loc[
        check_real_df[column].str.strip().str.len() == 7, column
    ].apply(
        lambda x: insert_space(x, 4)
    )

    # Add 2 whitespaces to 6 digit postcodes
    check_real_df.loc[check_real_df[column].str.strip().str.len() == 6, column] = (
        check_real_df.loc[check_real_df[column].str.strip().str.len() == 6, column]
        .apply(lambda x: insert_space(x, 3))
        .apply(lambda x: insert_space(x, 4))
    )

    # Add 3 whitespaces to 5 digit postcodes
    check_real_df.loc[check_real_df[column].str.strip().str.len() == 5, column] = (
        check_real_df.loc[check_real_df[column].str.strip().str.len() == 5, column]
        .apply(lambda x: insert_space(x, 2))
        .apply(lambda x: insert_space(x, 3))
        .apply(lambda x: insert_space(x, 4))
    )

    return check_real_df


def check_pcs_real(
    df: pd.DataFrame,
    check_real_df: pd.DataFrame,
    postcode_masterlist: pd.DataFrame,
    config: dict,
):
    """Checks if the postcodes are real against a masterlist of actual postcodes.

    In the masterlist, all postcodes are 7 characters long, therefore the
    reference are formatted to match this format.

    All postcodes above 7 characters are stripped of whitespaces.
    All postcodes less than 7 characters have added whitespaces in the middle.

    This formatting is applied to a copy dataframe so the original is unchanged.

    The final output are the postcodes from the original dataframe

    Args:
        df (pd.DataFrame): The DataFrame containing the postcodes.
        check_real_df (pd.DataFrame): The DataFrame excluding invalid postcodes, to
        run the comparison against
        postcode_masterlist (pd.DataFrame): The dataframe containing the correct
        postocdes to check against

    Returns:
        unreal_postcodes (pd.DataFrame): A dataframe containing all the
        original postcodes not found in the masterlist

    """

    if config["global"]["postcode_csv_check"]:
        master_series = get_masterlist(postcode_masterlist)

        # Check if postcode are real
        check = check_real_df[
            ~check_real_df["postcodes_harmonised"].isin(master_series)
        ]
        unreal_postcodes = df.loc[check.index, "postcodes_harmonised"]

    else:
        emptydf = pd.DataFrame(columns=["postcodes_harmonised"])
        unreal_postcodes = emptydf.loc[
            ~emptydf["postcodes_harmonised"], "postcodes_harmonised"
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
    raise_error=False,
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
        ValidationLogger.info(f"Data columns match schema.")
    else:
        cols_match = False
        ValidationLogger.warning(f"Data columns do not match schema.")
        missing_file_cols = (
            f"Missing from dataframe: {schema_full_col_set - df_cols_set}"
        )
        missing_df_cols = f"Missing from schema: {df_cols_set - schema_full_col_set}"
        ValidationLogger.warning(missing_file_cols)
        ValidationLogger.warning(missing_df_cols)
        if raise_error:
            raise ColumnMismatch(
                "Error: The the number of columns do not match. Halted"
            )

    ValidationLogger.info(
        f"Length of data: {len(df_cols_set)}. Length of schema: {len(schema_full_col_set)}"
    )

    return cols_match


@time_logger_wrap
def validate_data_with_schema(survey_df: pd.DataFrame, schema_path: str):
    """Takes the schema from the toml file and validates the survey data df.

    Args:
        survey_df (pd.DataFrame): Survey data in a pd.df format
        schema_path (str): path to the schema toml (should be in config folder)
    """
    ValidationLogger.info(f"Starting validation with {schema_path}")
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
            survey_df[column] = np.nan
            dtypes_dict[column] = "float64"

        try:
            ValidationLogger.debug(f"{column} before: {survey_df[column].dtype}")
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
            ValidationLogger.debug(f"{column} after: {survey_df[column].dtype}")
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

        ValidationLogger.debug(f"{column} before: {survey_df[column].dtype}")
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
        ValidationLogger.debug(f"{column} after: {survey_df[column].dtype}")
    ValidationLogger.info("Finished data type casting process")


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


@time_logger_wrap
@exception_wrap
def validate_many_to_one(
    mapper: pd.DataFrame, col_many: str, col_one: str
) -> pd.DataFrame:
    """

    Validates a many to one mapper:
    1. Checks if the mapper has two columns col_many and col_one.
    2. Salects and deduplicates col_many and col_one.
    3. Checks that for each entry in col_many there is exactly one entry in
    col_one.

    Args:
        df (pd.DataFrame): The input mapper
        col_many (str): name of the column with many entries
        col_one (str): name of the column with one entry
    """
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
            raise ValueError(f"Mapper is many to many")
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
        if (df["contents_check"] == False).any():
            raise ValueError("Unexpected format within column contents")

        # Drop the "contents_check" column
        df.drop(columns=["contents_check"], inplace=True)

        return df

    except ValueError as ve:
        raise ValueError("cora status mapper validation failed: " + str(ve))


def flag_no_rand_spenders(df, raise_or_warn):
    """
    Flags any records that answer "No" to "604" and also report their expenditure in "211" as more than 0.

    Parameters:
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
        ValidationLogger.debug("All records have valid R&D spend.")
