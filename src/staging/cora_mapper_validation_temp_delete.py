import logging
import pandas as pd

from src.utils.wrappers import exception_wrap

validation_logger = logging.getLogger(__name__)


@exception_wrap
def validate_cora_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates cora mapper df:
    1. Checks if the DataFrame has exactly two columns.
    2. Checks if the column headers are "statusencoded" and "form_status".
    3. Checks the validity of the columns contents.
    Args:
        df (pd.DataFrame): The input DataFrame containing "statusencoded"
        and "form_status" columns.
    Returns:
        df (pd.DataFrame): with cols changed to string type
    """
    try:
        # Check if the dataframe has exactly two columns
        if df.shape[1] != 2:
            raise ValueError("DataFrame must have exactly two columns")

        # Check if the column headers are "statusencoded" and "form_status"
        if list(df.columns) != ["statusencoded", "form_status"]:
            raise ValueError("Column headers must be 'statusencoded' and 'form_status'")

        # Check the contents of the "status" and "form_status" columns
        status_check = df["statusencoded"].str.len() == 3
        from_status_check = df["form_status"].str.len().isin([3, 4])

        # Create the "contents_check" column based on the checks
        df["contents_check"] = status_check & from_status_check

        # Check if there are any False values in the "contents_check" column
        if (df["contents_check"] == False).any():
            raise ValueError("Unexpected format within column contents")

        # Drop the "contents_check" column
        df.drop(columns=["contents_check"], inplace=True)

        return df

    except ValueError as ve:
        raise ValueError("cora status mapper validation failed: " + str(ve))
