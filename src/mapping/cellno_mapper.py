"""Return a dictionary for the cell_no mapper"""
import pandas as pd

from src.mapping.mapping_helpers import check_mapping_unique


def clean_thousands_comma(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """Remove commas from numbers in a column of a dataframe and convert to integer."""
    df[col_name] = df[col_name].str.replace(",", "").astype(int)
    return df


def check_expected_number_of_cellnumbers(cellno_df: pd.DataFrame, num: int) -> None:
    """Check we have the expected number of cellnumers.

    Parameters:
    cellno_df (pd.DataFrame): The cellnumber mapper dataframe.
    num (int): The expected number of cellnumbers.

    Raises:
    ValueError: If the number of cellnumbers is not as expected.
    """
    # check the number of cellnumbers
    if len(cellno_df) != num:
        raise ValueError(
            f"Coverage mapper does not have the expected {num} number of cellnumbers."
        )


def check_cellno_range(cellno_df: pd.DataFrame) -> None:
    """Check the range of the cellnumbers is as expected.

    Parameters:
    cellno_df (pd.DataFrame): The cellnumber mapper dataframe.

    Raises:
    ValueError: If the cellnumbers are not in the expected range.
    """
    if not cellno_df["cell_no"].between(1, 817).all():
        raise ValueError("Cellnumbers are not in the expected range of 1 to 817.")


def clean_validate_cellno_mapper(cellno_df: pd.DataFrame, num: int) -> pd.DataFrame:
    """Clean and validate the cellno mapper dataframe.

    Parameters:
    cellno_df (pd.DataFrame): The cellnumber mapper dataframe.
    num (int): The expected number of cellnumbers.

    Returns:
    pd.DataFrame: The cleaned and validated cellnumber mapper dataframe.

    Raises:
    ValueError: If the number of cellnumbers is not as expected.
    ValueError: If the cellnumbers are not in the expected range.
    """
    # check for unique cellnumbers
    check_mapping_unique(cellno_df, "cell_no")
    # check the number of cellnumbers
    check_expected_number_of_cellnumbers(cellno_df, num)
    # check the range of the cellnumbers
    check_cellno_range(cellno_df)

    cellno_df = cellno_df.copy()[["cell_no", "UNI_Count"]]
    cellno_df = cellno_df.rename(
        columns={"UNI_Count": "uni_count", "cell_no": "cellnumber"}
    )
    cellno_df = clean_thousands_comma(cellno_df, "uni_count")
    return cellno_df


def join_cellno_mapper(df: pd.DataFrame, cellno_df: pd.DataFrame) -> pd.DataFrame:
    """Add a column for universe count to shortfrom responses, joining on cellnumber.

    Parameters:
    df (pd.DataFrame): The shortform responses dataframe.

    Returns:
    pd.DataFrame: The shortform responses dataframe with a column for universe count.

    """
    df = df.merge(cellno_df, on="cellnumber", how="left")

    return df


def validate_join_cellno_mapper(
    df: pd.DataFrame, cellno_df: pd.DataFrame
) -> pd.DataFrame:
    """Validate the join_cellno_mapper function.

    Parameters:
    df (pd.DataFrame): The shortform responses dataframe.
    cellno_df (pd.DataFrame): The cellnumber mapper dataframe.

    Returns:
    pd.DataFrame: The shortform responses dataframe with a column for universe count.
    """
    cellno_df = clean_validate_cellno_mapper(cellno_df, num=613)
    df = join_cellno_mapper(df, cellno_df)

    return df
