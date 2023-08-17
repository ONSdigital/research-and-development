import pandas as pd


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
