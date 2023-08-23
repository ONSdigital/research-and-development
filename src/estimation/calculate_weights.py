import pandas as pd
import numpy as np
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
    if not ("auto_outlier" in cols) & ("manual_outlier" in cols):
        raise ValueError(
            "Either automatic_outlier or manual_outlier are not present in df"
        )

    # Create a new col "is_outlier" using automatic_outlier as main source of
    # logic and manual as override
    df["is_outlier"] = df["manual_outlier"].combine_first(df["auto_outlier"])

    return df


def calc_lower_n(df: pd.DataFrame, exp_col: str = "709") -> dict:
    """Calculates 'n' which is a number of unique RU references that have
        positive (non-negative) non-Null expenditure.

    Args:
        df (pd.DatatFrame): The input dataframe which contains survey data,
            including expenditure data
        exp_col (str): An appropriate column to count n

    Returns:
        int: The number of unique references that have positive (non-negative)
            non-Null expenditure.
    """

    # Check if any of the key cols are missing
    cols = set(df.columns)
    if not ("reference" in cols) & (exp_col in cols):
        raise ValueError(f"'reference' or {exp_col} missing.")

    # Filter out 0 and null vals
    df_filtered = df[df[exp_col] > 0]
    df_filtered = df_filtered.dropna(subset=[exp_col])

    # Count the filtered records
    n = df_filtered["reference"].nunique()

    return n


def calculate_weighting_factor(df: pd.DataFrame, cellno_dict) -> dict:
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

    # Make a copy of the dataframe
    # df = df.copy()

    # Create a new blank column with a_weight as name
    df["a_weight"] = np.nan
    
    # Filter for shortforms with 'P' seltype
    
    filtered_df = df[
        (df["formtype"] == '0006') & (df["selectiontype"] == 'P')
    ]
    
    # Filter out 0 and null vals for column 709
    filtered_df = filtered_df[filtered_df['709'] > 0]
    filtered_df = filtered_df.dropna(subset=['709'])
    
    # Filter for clean statuses 211, 210 statusencoded
    
    filtered_df = filtered_df[filtered_df["statusencoded"].isin(['210','211'])]
    
    # Default a_weight for filtered_df = 1
    
    filtered_df["a_weight"] = 1.0
    
    # Create small QA dataframe
    
    qa_df = pd.DataFrame({'cellnumber': [],
                          'N': [],
                          'n': [],
                          'outliers': [],
                          'a_weight': [],
    })
    
    # Group by cell number
    groupd_by_cell = filtered_df.groupby("cellnumber")

    # Create a dict that maps each cell to the weighting factor

    for cell_number, cell_group in groupd_by_cell:

        # Get N from cellno_dict
        N = cellno_dict[cell_number]

        # Get lower n
        n = calc_lower_n(cell_group)

        # Count the outliers for this group (will count all the `True` values)
        outlier_count = cell_group["is_outlier"].sum()

        CalcWeights_Logger.info(
            "The number of outliers for cell number %s is %s", cell_number, outlier_count
        )
        CalcWeights_Logger.info("For cell number %s, N is %s and n is %s", cell_number, N, n)

        # Calculate 'a' for this group
        a_weight = (N - outlier_count) / (n - outlier_count)

        # Put the weight into the column just for this cell number
        df.loc[(df["cellnumber"] == cell_number), "a_weight"] = a_weight
        
        # Save the relevant estimation info for QA seperately.
        qa_list = [cell_number, N, n, outlier_count, a_weight]
        qa_df = qa_df.append(pd.Series(qa_list, index = qa_df.columns), ignore_index=True)

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

