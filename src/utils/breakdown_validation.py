"""Function to validate the breakdown totals."""
import logging
import pandas as pd

from typing import Tuple

BreakdownValidationLogger = logging.getLogger(__name__)


def get_equality_dicts(config: dict, sublist: str = "default") -> dict:
    """
    Get the equality checks for the construction data.

    Args:
        config (dict): The config dictionary.

    Returns:
        dict
    """
    # use the config to get breakdown totals
    all_checks_dict = config["consistency_checks"]

    # isolate the relationships suitlable for checking in the construction module
    if sublist == "default":
        wanted_dicts = [key for key in all_checks_dict.keys() if "xx_totals" in key]
    elif sublist == "imputation":
        wanted_dicts = ["2xx_totals", "3xx_totals", "apportioned_totals"]
    else:
        wanted_dicts = list(all_checks_dict.keys())

    # create a dictionary of the relationships to check
    construction_equality_checks = {}
    for item in wanted_dicts:
        construction_equality_checks.update(all_checks_dict[item])

    return construction_equality_checks


def get_all_wanted_columns(config: dict) -> list:
    """
    Get all the columns that we want to check.

    Args:
        config (dict): The config dictionary.

    Returns:
        list: A list of all the columns to check.
    """
    equals_checks = get_equality_dicts(config, "default")
    all_columns = []
    for list_item in equals_checks.values():
        all_columns += list_item
    return all_columns


def replace_nulls_with_zero(df: pd.DataFrame, equals_checks) -> pd.DataFrame:
    """
    Replace nulls with zeros where the total is zero.

    Args:
        df (pd.DataFrame): The dataframe to check.

    Returns:
        pd.DataFrame
    """
    BreakdownValidationLogger.info("Replacing nulls with zeros where total zero")

    for columns in equals_checks.values():
        total_column = columns[-1]
        breakdown_columns = columns[:-1]
        zero_mask = df[total_column] == 0
        for column in breakdown_columns:
            df.loc[zero_mask, column] = df.loc[zero_mask, column].fillna(0)
    return df


def remove_all_nulls_rows(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Remove rows where all breakdown/total cols are null from validation.

    Args:
        df (pd.DataFrame): The dataframe to check.
        config (dict): The pipeline config dictionary.

    Returns:
        pd.DataFrame
    """
    BreakdownValidationLogger.info("Removing rows with all null values from validation")
    wanted_cols = get_all_wanted_columns(config)
    rows_to_validate = df.dropna(
        subset=wanted_cols,
        how="all",
    ).reset_index(drop=True)

    return rows_to_validate


def equal_validation(
    rows_to_validate: pd.DataFrame, equals_checks: dict
) -> pd.DataFrame:
    """
    Check where the sum of some columns should equal another column.

    Args:
        rows_to_validate (pd.DataFrame): The dataframe to check.
        equals_checks (dict): The dictionary of columns to check.

    Returns:
        tuple(str, int)
    """
    BreakdownValidationLogger.info("Doing breakdown total checks...")

    msg = ""
    count = 0
    for index, row in rows_to_validate.iterrows():
        for key, columns in equals_checks.items():
            total_column = columns[-1]
            breakdown_columns = columns[:-1]
            if (
                rows_to_validate[columns].isnull().all(axis=1).iloc[index]
                or (rows_to_validate[columns] == 0).all(axis=1).iloc[index]
            ):
                continue
            if not (
                rows_to_validate[breakdown_columns].sum(axis=1)
                == rows_to_validate[total_column]
            ).iloc[index]:
                msg += (
                    f"Columns {breakdown_columns} do not equal column"
                    f" {total_column} for reference: {row['reference']}, instance"
                    f" {row['instance']}.\n "
                )
                count += 1
    return msg, count


def greater_than_validation(
    rows_to_validate: pd.DataFrame, msg: str, count: int
) -> pd.DataFrame:
    """
    Check where one value should be greater than another.

    Args:
        rows_to_validate (pd.DataFrame): The dataframe to check.
        msg (str): The message to append to.
        count (int): The count of errors.

    Returns:
        pd.DataFrame
    """
    BreakdownValidationLogger.info(
        "Doing checks for values that should be greater than..."
    )
    greater_than_checks = {
        "check14": ["209", "221"],
        "check15": ["211", "202"],
    }
    for index, row in rows_to_validate.iterrows():
        for key, columns in greater_than_checks.items():
            should_be_greater = columns[0]
            should_not_be_greater = columns[1]
            if (
                rows_to_validate[should_not_be_greater]
                > rows_to_validate[should_be_greater]
            ).all():
                msg += (
                    f"Column {should_not_be_greater} is greater than"
                    f" {should_be_greater} for reference: {row['reference']}, instance"
                    f" {row['instance']}.\n "
                )
                count += 1
    return msg, count


def get_breakdown_errors(df: pd.DataFrame, to_check: dict) -> pd.DataFrame:
    """Function to check total columns remain consistent after imputation.

    Args:
        df (pd.DataFrame): The dataframe to check.
        config (dict): The config dictionary.

    Returns:
        dict: A dictionary with boolean values for each check.
        pd.DataFrame: The dataframe with the breakdown errors
    """
    qa_df = df.copy()
    wanted_refs = []  # a list of references that have errors
    cols = []  # a list of columns that have errors

    check_results_dict = {}
    for key, columns in to_check.items():
        total_column = columns[-1]
        breakdown_columns = columns[:-1]
        check_cond = abs(df[breakdown_columns].sum(axis=1) - df[total_column]) > 0.005
        # if there are any errors for particular check..
        if any(check_cond):
            # ...create a mini dataframe for the relevant rows and columns
            wanted_cols = ["reference", "instance", "imp_class", "imp_marker"] + list(
                columns
            )
            check_df = df.copy().loc[check_cond, wanted_cols]
            check_df[f"{key}_diff"] = (
                df.loc[check_cond, breakdown_columns].sum(axis=1)
                - df.loc[check_cond, total_column]
            )

            # add the diff column to the qa dataframe
            qa_df.loc[check_cond, f"{key}_diff"] = check_df[f"{key}_diff"]
            wanted_refs += [
                r for r in (check_df["reference"].tolist()) if r not in wanted_refs
            ]
            cols += [c for c in check_df.columns if c not in cols] + [f"{key}_diff"]

            # add the mini-dataframe to the dict for unit testing and logger messages
            check_results_dict[key] = check_df
        else:
            check_results_dict[key] = pd.DataFrame()

    # Filter and select so the qa dataframe has only relevant columns and rows
    qa_df = qa_df.loc[df["reference"].isin(wanted_refs)][cols]

    return check_results_dict, qa_df


def log_errors_to_screen(check_results_dict: dict, check_type: str) -> None:
    """Function to log the errors to the screen.

    Args:
        check_results_dict (dict): The dictionary of errors to log.

    Returns:
        None
    """
    for key, value in check_results_dict.items():
        if not value.empty:
            BreakdownValidationLogger.error(
                f"Breakdown validation failed for {key} columns"
            )
            BreakdownValidationLogger.error(value)
    else:
        BreakdownValidationLogger.info(f"All {check_type} breakdown vals are valid.")


def run_imputation_breakdown_validation(df: pd.DataFrame, config: dict) -> None:
    """Function to run the breakdown validation for the imputed data.

    Args:
        df (pd.DataFrame): The dataframe to check.
        config (dict): The config dictionary.

    Raises:
        ValueError: If any of the breakdown values do not sum to the total.

    Returns:
        None
    """
    to_check_dict = get_equality_dicts(config, "imputation")
    check_results_dict, qa_df = get_breakdown_errors(df, to_check_dict)
    # temp output of qa_df for debugging
    if not qa_df.empty:
        print(qa_df)

    log_errors_to_screen(check_results_dict, "imputation")


def run_construction_breakdown_validation(df: pd.DataFrame, config: dict) -> None:
    """Function to run the breakdown validation for the constructed data.

    Few errors are expected, so any that exist will be logged to the screen.

    Args:
        df (pd.DataFrame): The dataframe to check.
        config (dict): The config dictionary.

    Returns:
        pd.DataFrame
    """
    to_check_dict = get_equality_dicts(config)
    df = replace_nulls_with_zero(df, to_check_dict)

    rows_to_validate = remove_all_nulls_rows(df, config)
    msg, count = equal_validation(rows_to_validate, to_check_dict)
    msg, count = greater_than_validation(rows_to_validate, msg, count)

    BreakdownValidationLogger.info(
        f"There are {count} errors with the breakdown values"
    )
    if not msg:
        BreakdownValidationLogger.info("All breakdown values are valid.")
    else:
        raise ValueError(msg)

    return df


def run_staging_breakdown_validation(df: pd.DataFrame, config: dict) -> None:
    """Function to run the breakdown validation for the staged data.
    Args:
        df (pd.DataFrame): The dataframe to check.
        config (dict): The config dictionary.

    Returns:
        None
    """
    to_check_dict = get_equality_dicts(config)
    df = replace_nulls_with_zero(df, to_check_dict)

    check_results_dict, qa_df = get_breakdown_errors(df, to_check_dict)

    # Note: this is a temporary implementation to show the QA output while we debug
    if not qa_df.empty:
        print(qa_df)

    log_errors_to_screen(check_results_dict, "staging")
    return df


def filter_on_condition(
    df: pd.DataFrame, condition: pd.Series
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Function to filter a dataframe based on a condition.

    Args:
        df (pd.DataFrame): The dataframe to filter.
        condition (pd.Series): The condition to filter on.

    Returns:
        tuple(pd.DataFrame, pd.DataFrame): The filtered dataframe and the dataframe that
          was removed.
    """
    validation_df = df[condition].copy()
    removed_df = df[~condition].copy()
    return validation_df, removed_df


def run_breakdown_validation(
    df: pd.DataFrame, config: dict, check: str
) -> pd.DataFrame:
    """
    Function to validate the breakdown totals.

    Args:
        df (pd.DataFrame): The dataframe to check.
        check (str): The type of validation to run. The values could be "staged",
            "imputed", or "constructed".

    Returns:
        pd.DataFrame or None
    """
    BreakdownValidationLogger.info(f"Running breakdown validation for {check} data")

    if check == "constructed":
        cond = df.is_constructed.isin([True])
        validation_df, remaining_df = filter_on_condition(df, cond)
        validation_df = run_construction_breakdown_validation(validation_df, config)

    elif check == "imputed":
        validation_df = df[df["imp_marker"].isin(["CF", "MoR", "TMI"])].copy()
        qa_df = run_imputation_breakdown_validation(validation_df, config)
        return qa_df

    elif check == "staged":
        cond = df.status.isin(["Clear", "Clear - overridden"])
        validation_df, remaining_df = filter_on_condition(df, cond)
        validation_df = run_staging_breakdown_validation(validation_df, config)

    else:
        raise ValueError("Check must be one of 'constructed', 'imputed', or 'staged'.")

    df = pd.concat([validation_df, remaining_df], ignore_index=True)
    return df
