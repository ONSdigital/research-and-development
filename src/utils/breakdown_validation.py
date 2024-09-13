"""Function to validate the breakdown totals."""
import logging
import pandas as pd

BreakdownValidationLogger = logging.getLogger(__name__)

def get_equality_dicts(config: dict, sublist: str="all") -> dict:
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
    if sublist == "construction":
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
    equals_checks = get_equality_dicts(config, "construction")
    all_columns = []
    for list_item in equals_checks.values():
        all_columns += list_item
    return all_columns


def replace_nulls_with_zero(df: pd.DataFrame, config) -> pd.DataFrame:
    """
    Replace nulls with zeros where the total is zero.

    Args:
        df (pd.DataFrame): The dataframe to check.

    Returns:
        pd.DataFrame
    """
    BreakdownValidationLogger.info("Replacing nulls with zeros where total zero")
    equals_checks = get_equality_dicts(config, "construction")

    for columns in equals_checks.values():
        total_column = columns[-1]
        breakdown_columns = columns[:-1]
        zero_mask = df[total_column] == 0
        for column in breakdown_columns:
            df.loc[zero_mask, column] = df.loc[zero_mask, column].fillna(0)
    return df


def remove_all_nulls_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows where all breakdown/total cols are null from validation.

    Args:
        df (pd.DataFrame): The dataframe to check.

    Returns:
        pd.DataFrame
    """
    BreakdownValidationLogger.info("Removing rows with all null values from validation")
    rows_to_validate = df.dropna(
        subset=[
            "222", "223", "203", "202", "204", "205", "206", "207", "221", "209", "219",  # noqa
            "220", "210", "204", "211", "212", "214", "216", "242", "250", "243", "244",  # noqa
            "245", "246", "218", "225", "226", "227", "228", "229", "237", "302", "303",  # noqa
            "304", "305", "501", "503", "505", "507", "502", "504", "506", "508", "405",  # noqa
            "407", "409", "411", "406", "408", "410", "412"  # noqa
        ], # noqa
        how="all",
    ).reset_index(drop=True)
    return rows_to_validate


def equal_validation(rows_to_validate: pd.DataFrame, config:dict) -> pd.DataFrame:
    """
    Check where the sum of some columns should equal another column.

    Args:
        rows_to_validate (pd.DataFrame): The dataframe to check.
        config (dict): The config dictionary.

    Returns:
        tuple(str, int)
    """
    BreakdownValidationLogger.info("Doing breakdown total checks...")
    equals_checks = get_equality_dicts(config, "construction")

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


def run_breakdown_validation(df: pd.DataFrame, config: dict, check: str = "all") -> pd.DataFrame:
    """
    Function to validate the breakdown totals.

    Args:
        df (pd.DataFrame): The dataframe to check.
        check (str): The type of validation to run. Default is 'all'.

    Returns:
        pd.DataFrame
    """

    if check == "constructed":
        BreakdownValidationLogger.info(
            "Running breakdown validation for constructed data"
        )
        validation_df = df[df["is_constructed"] == True].copy()
        not_for_validating_df = df[df["is_constructed"] == False].copy()
    else:
        validation_df = df.copy()

    validation_df = replace_nulls_with_zero(validation_df, config)
    rows_to_validate = remove_all_nulls_rows(validation_df)
    msg, count = equal_validation(rows_to_validate, config)
    msg, count = greater_than_validation(rows_to_validate, msg, count)

    if check != "all":
        df = pd.concat([validation_df, not_for_validating_df], ignore_index=True)
    else:
        df = validation_df

    BreakdownValidationLogger.info(
        f"There are {count} errors with the breakdown values"
    )

    if not msg:
        BreakdownValidationLogger.info("All breakdown values are valid.")
    else:
        raise ValueError(msg)

    return df
