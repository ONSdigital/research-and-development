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
    """This function checks if all postcodes in the specified DataFrame column 
        are valid UK postcodes. It uses the `validate_postcode` function to 
        perform the validation.

    Args:
        df (pd.DataFrame): The DataFrame containing the postcodes.

    Returns:
        bool: True if all postcodes are valid, False otherwise.

    Raises:
        ValueError: If any invalid postcodes are found, a ValueError is raised.
            The error message includes the list of invalid postcodes.

    Example:
        >>> df = pd.DataFrame({"referencepostcode": ["AB12 3CD", "EFG 456", "HIJ 789", "KL1M 2NO"]})
        >>> validate_post_col(df)
        ValueError: Invalid postcodes found: ['EFG 456', 'HIJ 789']
    """
    invalid_postcodes = df.loc[~df["referencepostcode"].apply(validate_postcode), "referencepostcode"]

    if not invalid_postcodes.empty:
        raise ValueError(f"Invalid postcodes found: {invalid_postcodes.to_list()}")
    
    return True
