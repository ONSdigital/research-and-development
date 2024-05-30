import os
import toml
import postcodes_uk
import pandas as pd
import numpy as np

import logging
from src.utils.wrappers import time_logger_wrap, exception_wrap

# Set up logging
ValidationLogger = logging.getLogger(__name__)


def check_log_invalid_postcodes(validation_df):

    # Apply the pattern validation function
    invalid_pattern_postcodes = validation_df.loc[
        ~validation_df["postcodes_harmonised"].apply(validate_postcode_pattern),
        "postcodes_harmonised",
    ]

    # Save to df
    invalid_df = create_issue_df(validation_df, invalid_pattern_postcodes, "invalid pattern / format")

    # Log the invalid postcodes
    ValidationLogger.warning(
        f"Invalid pattern postcodes found: {invalid_pattern_postcodes.to_list()}"
    )
    ValidationLogger.warning(
        f"""Number of invalid pattern postcodes found:
        {len(invalid_pattern_postcodes.to_list())}"""
    )
    return invalid_df, invalid_pattern_postcodes


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


def check_log_unreal_postcodes(validation_df, valid_postcode_pattern_df, postcode_masterlist, config):

    # Clean postcodes to match the masterlist
    valid_postcode_pattern_df["postcodes_harmonised"] = valid_postcode_pattern_df["postcodes_harmonised"].apply(format_postcodes)

    # Only validate not null postcodes for the column "601"
    valid_postcode_pattern_df = valid_postcode_pattern_df.loc[~valid_postcode_pattern_df["postcodes_harmonised"].isnull()]

    # Create a list of postcodes not found in masterlist in col "postcodes_harmonised"
    unreal_postcodes = check_pcs_real(
        valid_postcode_pattern_df, valid_postcode_pattern_df, postcode_masterlist, config
    )

    # Save to df
    unreal_df = create_issue_df(validation_df, unreal_postcodes, "not found in masterlist")

    # Log the unreal postcodes
    ValidationLogger.warning(
        f"These postcodes are not found in the ONS postcode list: {unreal_postcodes.to_list()}"  # noqa
    )

    ValidationLogger.warning(
        f"Number of postcodes not found in the ONS postcode list: {len(unreal_postcodes.to_list())}"  # noqa
    )
    return unreal_df, unreal_postcodes


def format_postcodes(postcode):

    if pd.notna(postcode):

        formatted_postcode = postcode.upper().strip().replace(" ", "")

        if len(formatted_postcode) >= 5:
            index = len(formatted_postcode) - 3
            spaces_needed = 8 - len(formatted_postcode)
            if spaces_needed > 0:
                return formatted_postcode[:index] + ' ' * spaces_needed + formatted_postcode[index:]
        elif len(formatted_postcode) < 5:
            spaces_needed = 8 - len(formatted_postcode)
            return formatted_postcode + ' ' * spaces_needed


@exception_wrap
def get_masterlist(postcode_masterlist) -> pd.Series:
    """This function converts the masterlist dataframe to a Pandas series

    Returns:
        pd.Series: A series of postcodes
    """
    masterlist = postcode_masterlist.squeeze()

    return masterlist


def check_pcs_real(
    df: pd.DataFrame,
    check_real_df: pd.DataFrame,
    postcode_masterlist: pd.DataFrame,
    config: dict,
):
    """Checks if the postcodes are real against a masterlist of actual postcodes.

    The checks are done on a copy of dataframe with the invalid postcodes removed.

    The final output are the postcodes from the original dataframe.

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


def create_issue_df(full_df, flagged_df, warning):
    issue_df = pd.DataFrame(
        {
            "reference": full_df.loc[flagged_df.index, "reference"],
            "instance": full_df.loc[flagged_df.index, "instance"],
            "formtype": full_df.loc[flagged_df.index, "formtype"],
            "postcode_issue": warning,
            "incorrect_postcode": flagged_df,
            "postcode_source": full_df.loc[
                flagged_df.index, "postcode_source"
            ],
        }
    )

    return issue_df


def combine_issue_postcodes(df, invalid_df, invalid_pattern_postcodes, unreal_df, unreal_postcodes):
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

    # update df to exclude any invalid/unreal postcode entries
    df["postcodes_harmonised"] = df["postcodes_harmonised"].where(
        ~df["postcodes_harmonised"].isin(
            combined_invalid_postcodes_df["incorrect_postcode"]
        ),
        other=None,
    )

    return df, combined_invalid_postcodes_df


@time_logger_wrap
@exception_wrap
def run_full_postcode_process(
    df: pd.DataFrame, postcode_masterlist: pd.DataFrame, config: dict
):
    """This function creates the postcodes_harmonised column containing
       valid UK postcodes only.

       The process is:
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

    # Check for invalid entries in postcode column
    invalid_df, invalid_pattern_postcodes = check_log_invalid_postcodes(validation_df)

    # Remove the invalid pattern postcodes before checking if they are real
    valid_postcode_pattern_df = validation_df.loc[
        ~validation_df.index.isin(invalid_pattern_postcodes.index.to_list())
    ]

    unreal_df, unreal_postcodes = check_log_unreal_postcodes(
        validation_df,
        valid_postcode_pattern_df,
        postcode_masterlist,
        config
        )

    df, combined_invalid_postcodes_df = combine_issue_postcodes(df, invalid_df, invalid_pattern_postcodes, unreal_df, unreal_postcodes)

    df["postcodes_harmonised"] = df["postcodes_harmonised"].apply(format_postcodes)
    df["601"] = df["601"].apply(format_postcodes)

    ValidationLogger.info("All postcodes validated....")

    return combined_invalid_postcodes_df

