"""Specific functions applied in mapping_main.py"""
import pandas as pd
import logging

MappingLogger = logging.getLogger(__name__)


def mapper_null_checks(
    mapper_df: pd.DataFrame, mapper_name: str, col1: str, col2: str
) -> None:
    """
    Perform null checks on two columns of a mapper DataFrame.

    Args:
        mapper_df (pd.DataFrame): The mapper DataFrame to check.
        mapper_name (str): The name of the mapper being validated.
        col1 (str): The name of the first column to check for nulls.
        col2 (str): The name of the second column to check for nulls.

    Raises:
        ValueError: If any null values are found in the mapper DataFrame.

    """
    if mapper_df[col1].isnull().any():
        raise ValueError(f"{mapper_name} mapper contains null values in {col1}.")
    if mapper_df[col2].isnull().any():
        raise ValueError(f"{mapper_name} mapper contains null values in {col2}.")


def join_with_null_check(
    df: pd.DataFrame,
    mapper_df: pd.DataFrame,
    mapper_name: str,
    join_col: str,
    warn: bool = False,
) -> pd.DataFrame:
    """Perform a left join on two DataFrames and check for nulls on the join.

    Args:
        df (pd.DataFrame): The main DataFrame.
        mapper_df (pd.DataFrame): The mapper DataFrame.
        mapper_name (str): The name of the mapper being validated.
        join_col (str): The column to join on.
        warn (bool, optional): Whether to warn instead of raising an error.

    Returns:
        pd.DataFrame: The merged DataFrame.

    Raises:
        ValueError: Raised if nulls are found in the join and 'warn' bool is False.

    """
    df = df.merge(
        mapper_df,
        how="left",
        on=join_col,
        indicator=True,
    )

    # Check for nulls in the join. Either warn or raise an error.
    filter_df = df.loc[df[join_col].notnull() & df._merge.eq("left_only")]
    if not filter_df.empty:
        msg = (
            f"Nulls found in the join on {join_col} of {mapper_name} mapper."
            f"The following {join_col} values are not in the {mapper_name} mapper: "
            f"{filter_df[join_col].unique()}"
        )
        if warn:
            MappingLogger.warning(msg)
        else:
            raise ValueError(msg)

    df = df.drop("_merge", axis=1)
    return df


def col_validation_checks(
    mapper_df: pd.DataFrame,
    mapper_name: str,
    col: str,
    expected_value_type: type = None,
    expected_value_length: int = None,
    check_capitalisation: bool = False,
) -> None:
    """
    Perform validation checks on a column in a DataFrame.

    This function checks the type, length, and capitalisation
    of a column in a DataFrame.
    It raises a ValueError if any of the checks fail.

    Args:
        mapper_df (pd.DataFrame): The mapper DataFrame to check.
        mapper_name (str): The name of the mapper being validated.
        col (str): The name of the column to check.
        expected_value_type (type, optional): The expected type of the column values.
        expected_value_length (int, optional): The expected length of the column values.
        check_capitalisation (bool, optional): Whether to check if the column
            values are in uppercase. Defaults to False.

    Raises:
        ValueError: If any of the validation checks fail.

    """
    try:
        if expected_value_type is not None:
            # Check datatype correct
            if (
                not mapper_df[col][mapper_df[col].notnull()]
                .apply(type)
                .eq(expected_value_type)
                .all()
            ):
                raise ValueError(
                    f"Column {col} is not of the expected type {expected_value_type}."
                )
        if expected_value_length is not None:
            # Convert to strings, and check if value lengths are correct
            if (
                not mapper_df[col]
                .loc[mapper_df[col].notnull()]
                .astype(str)
                .str.len()
                .eq(expected_value_length)
                .all()
            ):
                raise ValueError(
                    f"Column {col} does not have the expected length of"
                    f" {expected_value_length}."
                )
        if check_capitalisation:
            # Check if all non-null values in the column are in uppercase
            if not mapper_df[col][mapper_df[col].notnull()].str.isupper().all():
                raise ValueError(f"Column {col} is not in uppercase.")
    except ValueError as ve:
        raise ValueError(f"{mapper_name} mapper validation failed: " + str(ve))


def check_mapping_unique(
    mapper_df: pd.DataFrame,
    col_to_check: str,
) -> None:
    """
    Checks if a column contains unique values.

    Args:
        mapper_df (pd.DataFrame): The mapper DataFrame to check.
        col_to_check (str): The name of the column to check.

    Raises:
        ValueError: If the column does not contain unique values.
    """
    if not mapper_df[col_to_check].is_unique:
        raise ValueError(f"Column {col_to_check} is not unique.")


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


def create_additional_ni_cols(ni_full_responses: pd.DataFrame) -> pd.DataFrame:
    """
    Create additional columns for Northern Ireland data.

    Args:
        df (pd.DataFrame): The main DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with additional columns.
    """
    # Add required columns to NI data
    ni_full_responses["a_weight"] = 1
    ni_full_responses["604"] = "Yes"
    ni_full_responses["form_status"] = 600
    ni_full_responses["602"] = 100.0
    ni_full_responses["formtype"] = "0003"

    return ni_full_responses
