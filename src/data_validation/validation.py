import postcodes_uk


def validate_postcode(pcode: str) -> bool:
    """A function to validate UK postcodes which uses the

    Args:
        pcode (str): _description_

    Returns:
        bool: _description_
    """
    validation = postcodes_uk.validate(pcode)
    return validation
