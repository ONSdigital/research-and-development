import pandas as pd
import logging
from typing import Tuple


CalcWeights_Logger = logging.getLogger(__name__)


def calc_lower_n(df: pd.DataFrame, exp_col: str = "709") -> dict:
    """Calculates 'n' which is a number of
    unique RU references in the filtered dataset.

    Args:
        df (pd.DatatFrame): The input dataframe which contains survey data,
            including expenditure data
        exp_col (str): An appropriate column to count n

    Returns:
        int: The number of unique references.
    """

    # Check if any of the key cols are missing
    cols = set(df.columns)
    if not ("reference" in cols) & (exp_col in cols):
        raise ValueError(f"'reference' or {exp_col} missing.")

    # Count the records
    n = df["reference"].nunique()

    return n


def calculate_weighting_factor(
    df: pd.DataFrame, cellno_dict: dict, exp_col: str = "709"
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Calculate the weighting factor 'a' for each cell in the survery data

    Note: A 'cell' is a group of businesses.

    The calculation here is:

    a = (N-o) / (n-o)

    Where:
        - N is the total number of businesses in the cell
        - n is the number of businesses in sample for that cell
        - o is the number of outliers in the cell

    'o' is calculated in this function by summing all the `True` values
        because `True` == 1

    Args:
        df (pd.DataFrame): The input df containing survey data
        cellno_dict (dict): Dictionary of cellnumbers and UNI_counts
        exp_col (str, optional): The column that is used to calculate n.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]:
        1) Returns the full dataframe with the added
        new column "a_weight".
        2) Returns a QA dataframe of all variables used in the calculation
    """

    cols = set(df.columns)
    if not ("outlier" in cols):
        raise ValueError("The column essential 'outlier' is missing.")

    # Convert 709 column to numeric
    df["709"] = pd.to_numeric(df["709"], errors="coerce")

    # Default a_weight = 1 for all entries
    df["a_weight"] = 1.0

    # Create filters for dataframe
    sample_cond = df["selectiontype"] == "P"
    status_cond = df.statusencoded.isin(["210", "211"])
    formtype_cond = df["formtype"] == "0006"
    ins_cond = df["instance"] == 0

    filtered_df = df[formtype_cond & sample_cond & status_cond & ins_cond]
    filtered_df = filtered_df.dropna(subset=[exp_col])

    # Create small QA dataframe

    qa_frame = {
            "cellnumber": [],
            "N": [],
            "n": [],
            "outliers": [],
            "a_weight": [],
        }

    # Group by cell number
    grouped_by_cell = filtered_df.groupby("cellnumber")

    # Create a dict that maps each cell to the weighting factor
    for cell_number, cell_group in grouped_by_cell:

        # Get N from cellno_dict
        N = cellno_dict[cell_number]

        # Get lower n
        n = calc_lower_n(cell_group)

        # Count the outliers for this group (will count all the `True` values)
        outlier_count = cell_group["outlier"].sum()

        # Calculate 'a' for this group
        a_weight = (N - outlier_count) / (n - outlier_count)

        # Put the weight into the column just for this cell number and filters
        cell_cond = df["cellnumber"] == cell_number
        df.loc[
            formtype_cond & sample_cond & status_cond & cell_cond,
            "a_weight",
        ] = a_weight

        # Save the relevant estimation info for QA seperately.
        qa_list = [
            float(cell_number),
            float(N), 
            float(n),
            float(outlier_count), 
            a_weight
        ]
        for col, val in zip(list(qa_frame.keys()), qa_list):
            qa_frame[col].append(val)

    qa_df = pd.DataFrame(qa_frame)
    return df, qa_df


def outlier_weights(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate weights for outliers.

    If a reference has been flagged as an outlier,
    the 'a weight' value is set to 1.0

    Args:
        df (pd.DataFrame): The dataframe weights are calculated for.

    Returns:
        pd.DataFrame: The dataframe with the a_weights set to 1.0 for outliers.
    """
    df.loc[df["outlier"], "a_weight"] = 1.0
    return df
