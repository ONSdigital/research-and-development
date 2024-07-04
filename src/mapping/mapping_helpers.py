"""Specific functions applied in mapping_main.py"""
import pandas as pd
import logging

MappingLogger = logging.getLogger(__name__)


def col_validation_checks(
    df: pd.DataFrame,
    mapper_name: str,
    col: str,
    expected_value_type: type,
    expected_value_length: int,
    check_capitalisation: bool = False) -> None:
    """
    Perform validation checks on a column in a DataFrame.

    This function checks the type, length, and capitalisation of a column in a DataFrame.
    It raises a ValueError if any of the checks fail.

    Args:
        df (pd.DataFrame): The DataFrame to check.
        mapper_name (str): The name of the mapper being validated.
        col (str): The name of the column to check.
        expected_value_type (type): The expected type of the column values.
        expected_value_length (int): The expected length of the column values.
        check_capitalisation (bool, optional): Whether to check if the column values are in uppercase. Defaults to False.

    Raises:
        ValueError: If any of the validation checks fail.

    """
    try:
        if expected_value_type is not None:
            # Filter out nulls before checking the type
            if not df[col][df[col].notnull()].apply(type).eq(expected_value_type).all():
                raise ValueError(f"Column {col} is not of the expected type {expected_value_type}.")
        if expected_value_length is not None:
            # Convert all non-null values to strings, then check if value lengths are correct
            if not df[col][df[col].notnull()].astype(str).str.len().eq(expected_value_length).all():
                raise ValueError(f"Column {col} does not have the expected length of {expected_value_length}.")
        if check_capitalisation:
            # Check if all non-null values in the column are in uppercase
            if not df[col][df[col].notnull()].str.isupper().all():
                raise ValueError(f"Column {col} is not in uppercase.")
    except ValueError as ve:
        raise ValueError(f"{mapper_name} mapper validation failed: " + str(ve))


def check_mapping_unique(
    df: pd.DataFrame,
    col_to_check: str,
) -> None:
    """
    Checks if a column contains unique values.

    Parameters:
    df (pd.DataFrame): The DataFrame to check.
    col_to_check (str): The name of the column to check.

    Raises:
    ValueError: If the column does not contain unique values.
    """
    if not df[col_to_check].is_unique:
        raise ValueError(f"Column {col_to_check} is not unique.")


def join_fgn_ownership(
    df: pd.DataFrame,
    mapper_df: pd.DataFrame,
    is_northern_ireland: bool = False,
) -> pd.DataFrame:
    """
    Combine two DataFrames using a left join based on specified columns.

    Args:
        df (pd.DataFrame): The main DataFrame.
        mapper_df (pd.DataFrame): The mapper DataFrame.

    Returns:
        pd.DataFrame: The combined DataFrame resulting from the left join.
    """

    if is_northern_ireland:
        df = df.rename(columns={"foc": "ultfoc"})
        df["ultfoc"] = df["ultfoc"].fillna("GB")
        mapped_df["ultfoc"] = mapped_df["ultfoc"].replace("", "GB")
        return df

    else:
        mapped_df = df.merge(
            mapper_df,
            how="left",
            left_on="reference",
            right_on="ruref",
        )
        mapped_df.drop(columns=["ruref"], inplace=True)
        mapped_df["ultfoc"] = mapped_df["ultfoc"].fillna("GB")
        mapped_df["ultfoc"] = mapped_df["ultfoc"].replace("", "GB")
        return mapped_df


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
