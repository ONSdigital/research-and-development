import postcodes_uk


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
