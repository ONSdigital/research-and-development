import pandas as pd
import logging


CalcWeights_Logger = logging.getLogger(__name__)


def check_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Check if a records is an outlier based on the automatic_outlier
        and manual_outlier column.

    Uses automatic_outlier as the main source of information with manual_outlier
        being the outlier "trump card", meaning it will override the auto column.

    | automatic_outlier | manual_outlier | is_outlier |
    |-------------------|----------------|------------|
    | True              | True           | True       |
    | True              | False          | False      |
    | False             | True           | True       |
    | False             | False          | False      |
    | True              | Null           | True       |
    | False             | Null           | False      |

    The last two rows in the example table will represent the logic in
        most cases as most will not be manually overidden.

    Args:
        df (pd.DataFrame): The input DataFrame containing the survey data

    Returns:
        pd.DataFrame: The input DataFrame with an additional column "is_outlier"
            which is the result of the above logic
    """

    # Check if the automatic and manual cols are present
    cols = set(df.columns)
    if not ("automatic_outlier" in cols and "manual_outlier" in cols):
        raise ValueError(
            "Either automatic_outlier or manual_outlier are not present in df"
        )

    # Create a new col "is_outlier" using automatic_outlier as main source of
    # logic and manual as override
    df["is_outlier"] = df["manual_outlier"].combine_first(df["automatic_outlier"])

    return df


def calculate_weighting_factor(df: pd.DataFrame) -> dict:
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

    Returns:
        weighting_factor_dict (dict): The input df with an aditional column

    """

    cols = set(df.columns)
    if not ("is_outlier" in cols):
        raise ValueError("The column essential 'is_outlier' is missing.")

    # Group by cell number
    groupd_by_cell = df.groupby("cell_no")

    # Create a dict that maps each cell to the weighting factor
    weighting_factors_dict = {}
    for name, cell_group in groupd_by_cell:
        # Get name for the dict key
        cell_name = tuple(name)

        # Count the outliers for this group (will count all the `True` values)
        outlier_count = cell_group["is_outlier"].sum()

        CalcWeights_Logger.debug(
            "The number of outliers in %s is %s", name, outlier_count
        )

        # Calculate 'a' for this group
        weighting_factors_dict[cell_name] = (
            cell_group["N_count"]
            - outlier_count / cell_group["n_count"]
            - outlier_count
        )

    return weighting_factors_dict
