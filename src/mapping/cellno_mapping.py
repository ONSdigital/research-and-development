"""Functions to clean and validate the cell no mapper."""
import pandas as pd
import logging

from typing import Tuple

from src.mapping.mapping_helpers import check_mapping_unique, join_with_null_check

MappingLogger = logging.getLogger(__name__)


def clean_validate_cellno_mapper(cellno_df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate the cellno mapper dataframe.

    Args:
        cellno_df (pd.DataFrame): The cellnumber mapper dataframe.

    Returns:
        pd.DataFrame: The cleaned and validated cellnumber mapper dataframe.

    Raises:
        ValueError: If the cellnumbers are not in the expected range.
    """
    check_mapping_unique(cellno_df, "cell_no")

    if not cellno_df["cell_no"].between(1, 817).all():
        raise ValueError("Cellnumbers are not in the expected range of 1 to 817.")

    cellno_df = cellno_df.copy()[["cell_no", "UNI_Count"]]
    cellno_df = cellno_df.rename(
        columns={"UNI_Count": "uni_count", "cell_no": "cellnumber"}
    )
    return cellno_df


def validate_join_cellno_mapper(
    responses: Tuple[pd.DataFrame, pd.DataFrame], cellno_df: pd.DataFrame, config: dict
) -> pd.DataFrame:
    """Validate the join_cellno_mapper function.

    Args:
        responses (Tuple[pd.DataFrame, pd.DataFrame]): The GB & NI responses dataframes
        cellno_df (pd.DataFrame): The cellnumber mapper dataframe.
        config (dict): The configuration dictionary.

    Returns:
        pd.DataFrame: The shortform responses dataframe with a column for universe count
    """
    gb_df, ni_df = responses
    cellno_df = clean_validate_cellno_mapper(cellno_df)
    gb_df = join_with_null_check(gb_df, cellno_df, "cellno", "cellnumber")

    return gb_df, ni_df
