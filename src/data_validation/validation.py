import postcodes_uk
import pandas as pd

# import pandera


def validate_postcode(pcode: str) -> bool:
    """A function to validate UK postcodes which uses the

    Args:
        pcode (str): The postcode to validate

    Returns:
        bool: True or False depending on if it is valid or not
    """
    # Validation step
    valid_bool = postcodes_uk.validate(pcode)

    return valid_bool


def validate_post_col(df: pd.Dataframe) -> bool:
    """_summary_

    Args:
        df (pd.Dataframe): _description_

    Returns:
        bool: _description_
    """
    bool_series = df.referencepostcode.apply(validate_postcode)

    whole_col_valid = bool_series.all()

    return whole_col_valid
