import postcodes_uk
import pandas as pd

from src.utils.wrappers import logger_creator
from src.utils.helpers import Config_settings


# Get the config
conf_obj = Config_settings()
config = conf_obj.config_dict
global_config = config["global"]

# Set up logging
logger = logger_creator(global_config)


def validate_postcode_pattern(pcode: str) -> bool:
    """A function to validate UK postcodes which uses the

    Args:
        pcode (str): The postcode to validate

    Returns:
        bool: True or False depending on if it is valid or not
    """
    if pcode is None:
        return False

    # Validation step
    valid_bool = postcodes_uk.validate(pcode)

    return valid_bool


def get_masterlist(masterlist_path) -> pd.Series:
    """This function loads the masterlist of postcodes from a csv file

    Returns:
        pd.Series: The dataframe of postcodes
    """
    masterlist = pd.read_csv(masterlist_path, usecols=["pcd"]).squeeze()
    return masterlist


def validate_post_col(df: pd.DataFrame, masterlist_path: str) -> bool:
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
        >>> df = pd.DataFrame(
            {"referencepostcode": ["AB12 3CD", "EFG 456", "HIJ 789", "KL1M 2NO"]})
        >>> validate_post_col(df, "example-path/to/masterlist.csv"")
        ValueError: Invalid postcodes found: ['EFG 456', 'HIJ 789']
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"The dataframe you are attempting to validate is {type(df)}")

    if config["global"]["postcode_csv_check"]:
        master_series = get_masterlist(masterlist_path)

        # Check if postcode are real
        unreal_postcodes = df.loc[
            ~df["referencepostcode"].isin(master_series), "referencepostcode"
        ]
    else:
        unreal_postcodes = pd.DataFrame([])

    # Log the unreal postcodes
    if not unreal_postcodes.empty:
        logger.warning(
            f"These postcodes are not found in the ONS postcode list: {unreal_postcodes.to_list()}"  # noqa
        )

    # Check if postcodes match pattern
    invalid_pattern_postcodes = df.loc[
        ~df["referencepostcode"].apply(validate_postcode_pattern), "referencepostcode"
    ]

    # Log the invalid postcodes
    if not invalid_pattern_postcodes.empty:
        logger.warning(
            f"Invalid pattern postcodes found: {invalid_pattern_postcodes.to_list()}"
        )

    # Combine the two lists
    combined_invalid_postcodes = pd.concat(
        [unreal_postcodes, invalid_pattern_postcodes]
    )
    combined_invalid_postcodes.drop_duplicates(inplace=True)

    if not combined_invalid_postcodes.empty:
        raise ValueError(
            f"Invalid postcodes found: {combined_invalid_postcodes.to_list()}"
        )

    return True
