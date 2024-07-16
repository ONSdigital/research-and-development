"""Functions to clean and validate the cell no mapper."""
import pandas as pd

from src.mapping.mapping_helpers import check_mapping_unique, join_with_null_check


def clean_validate_cellno_mapper(cellno_df: pd.DataFrame, num: int) -> pd.DataFrame:
    """Clean and validate the cellno mapper dataframe.

    Args:
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
    if len(cellno_df) != num:
        raise ValueError(
            f"Coverage mapper does not have the expected {num} number of cellnumbers."
        )
    # check the range of the cellnumbers
    if not cellno_df["cell_no"].between(1, 817).all():
        raise ValueError("Cellnumbers are not in the expected range of 1 to 817.")

    cellno_df = cellno_df.copy()[["cell_no", "UNI_Count"]]
    cellno_df = cellno_df.rename(
        columns={"UNI_Count": "uni_count", "cell_no": "cellnumber"}
    )
    return cellno_df


def validate_join_cellno_mapper(
    df: pd.DataFrame, cellno_df: pd.DataFrame, config: dict
) -> pd.DataFrame:
    """Validate the join_cellno_mapper function.

    Args:
        df (pd.DataFrame): The shortform responses dataframe.
        cellno_df (pd.DataFrame): The cellnumber mapper dataframe.
        config (dict): The configuration dictionary.

    Returns:
        pd.DataFrame: The shortform responses dataframe with a column for universe count
    """
    num = config["estimation"]["num_expected_cellnos"]
    cellno_df = clean_validate_cellno_mapper(cellno_df, num)
    df = join_with_null_check(df, cellno_df, "cellno", "cellnumber")

    return df
