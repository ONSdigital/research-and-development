"""Simple utils to assist the config."""
from copy import deepcopy
from typing import Union, Dict

from src.utils.defence import type_defence, validate_file_extension
from src.utils.local_file_mods import safeload_yaml
from src.utils.path_helpers import update_config_with_paths


def config_setup(user_config_path: str, dev_config_path: str) -> Dict:
    """Set up the config for the pipeline.

    Args:
        user_config_path (str): The path to the user config file.
        dev_config_path (str): The path to the developer config file.

    Returns:
        Dict: The merged user and developer configs.
    """
    user_config, dev_config = load_validate_configs(user_config_path, dev_config_path)
    combined_config = merge_configs(user_config, dev_config)
    del user_config, dev_config

    # update the config with the full paths
    modules = [
        "imputation",
        "outliers",
        "estimation",
        "apportionment",
        "outputs",
    ]
    combined_config = update_config_with_paths(combined_config, modules)

    return combined_config


def load_validate_configs(user_config_path: str, dev_config_path: str):
    """Load and validate the user and developer configs.

    Args:
        user_config_path (str): The path to the user config file.
        dev_config_path (str): The path to the developer config file.

    Returns:
        Tuple[Dict, Dict]: The user and developer configs.
    """
    user_config = safeload_yaml(user_config_path)
    dev_config = safeload_yaml(dev_config_path)
    if user_config["config_validation"]["validate"]:
        validate_config(user_config)
        validate_freezing_config_settings(user_config)
    if dev_config["config_validation"]["validate"]:
        validate_config(dev_config)

    # drop validation keys
    user_config.pop("config_validation", None)
    dev_config.pop("config_validation", None)
    return user_config, dev_config


def merge_configs(config1: Dict, config2: Dict) -> Dict:
    """Merge two config files.

    Takes two config files and merges them into a single config file.
    If there are overlapping keys in the configs, a ValueError is raised.

    Args:
        config1 (dict): The first config file.
        config2 (dict): The second config file.

    Raises:
        ValueError: Raised if there are overlapping keys in the configs.

    Returns:
        dict: The merged config file.
    """
    for key in config1:
        if key in config2:
            # check if the value is a dict and if so, merge the subkeys
            # if it is not a dict, there is overlap so raise an error
            if isinstance(config2[key], dict) and isinstance(config1[key], dict):
                for subkey in config1[key]:
                    # if the subkey is also in the second config, then there is overlap
                    if subkey not in config2[key].keys():
                        config2[key][subkey] = config1[key][subkey]
                    else:
                        raise ValueError(f"Overlapping keys in configs: {key}:{subkey}")
            else:
                raise ValueError(f"Overlapping keys in configs: {key}")

        else:
            config2[key] = config1[key]
    return deepcopy(config2)


def _check_has_schema(config_item: dict) -> bool:
    """Check if a validation config item is a schema.

    This is done by seeing whether the item has the required keys.

    Args:
        config_item (dict): An item from the validation schema.

    Returns:
        bool: Whether config item has required keys.
    """
    expected_keys = [
        "singular",
        "dtype",
        "accept_nonetype",
    ]
    if set(expected_keys).issubset(set(list(config_item.keys()))):
        return True
    return False


def _validate_path(path: str, schema_config: dict):
    """Check a path has the extension specified in the config schema.

    Args:
        path (str): The path to validate.
        schema_config (dict): The config schema containing the file extension.

    Raises:
        KeyError: If the validation schema config does not contain the 'filetype' key.

    Returns:
        None
    """
    try:
        file_ext = schema_config["filetype"]
    except KeyError:
        file_ext = None
    if file_ext:
        validate_file_extension(path, file_ext)


def _validate_numeric(value: Union[float, int], param_nm: str, config: dict):
    """Validate a numerical value to ensure it falls within a specified range.

    Args:
        value (Union[float, int]): The numerical value to be validated.
        param_nm (str): The name of the parameter being validated (for error messages).
        config (dict): The configuration schema containing the min and max values.

    Raises:
        ValueError: If the value is greater than the maximum specified in the config.
        ValueError: If the value is less than the minimum specified in the config.
    """
    try:
        max_value = config["max"]
    except KeyError:
        max_value = None
    try:
        min_value = config["min"]
    except KeyError:
        min_value = None
    if max_value:
        if value > max_value:
            raise ValueError(
                f"Config value for {param_nm} ({value}) greater than max ({max_value})."
            )
    if min_value:
        if value < min_value:
            raise ValueError(
                f"Config value for {param_nm} ({value}) less than min ({min_value})."
            )


def _nulltype_conversion(value: str) -> Union[str, None]:
    """Account for user disparities in the form of NoneType they pass.

    Args:
        value (str): The value passed by a user.

    Returns:
        Union[str, None]: The altered value.
    """
    type_defence(value, "value", str)
    if value.lower() in ["", "null", "none"]:
        return None
    return value


def _validate_config_items(  # noqa C901
    config_item: dict, validation_item: dict, item_name: str
) -> None:
    """Recursively validate items in a config.

    This function recursively checks items in the config and validates them. It
    traverses the tree of the config (levels) until it reaches a validation schema.
    If a validation schema is detected, the function will stop recursively going
    deeper into the tree and validate the current level with the parameters passed.

    Args:
        config_item (dict): The config item to be validated.
        validation_item (dict): The dict to use for validation.
        item_name (str): The name of the item (matching in config and validation items)
    """
    DTYPES = {
        "int": int,
        "float": float,
        "str": str,
        "bool": bool,
        "path": str,  # passed as string in config
        "list": list,
    }
    # check whether we have reached a schema or need to traverse deeper
    if not _check_has_schema(config_item=validation_item):
        # traverse deeper by calling the function recursively
        for sub_item in validation_item.keys():
            _validate_config_items(
                config_item=config_item[sub_item],
                validation_item=validation_item[sub_item],
                item_name=sub_item,
            )
        return
    # convert a "singular" item to a dictionary so it can be processed
    # in the same way as non-singular items.
    config_item = (
        {item_name: config_item} if validation_item["singular"] else config_item
    )
    dtype = (
        DTYPES[validation_item["dtype"]]
        if "list" not in validation_item["dtype"]
        else list
    )
    dtype = (dtype, type(None)) if validation_item["accept_nonetype"] else dtype
    for config_value in config_item.keys():
        # create the parameter name for the error message
        param = f"{item_name}:{config_value}"
        # check if the item is a list and validate each item in the list
        if "list" in validation_item["dtype"]:
            dtype = validation_item["dtype"]
            type_defence(config_item[config_value], param, list)
            subtype = dtype.replace("list[", "").replace("]", "")
            for list_item in config_item[config_value]:
                type_defence(
                    list_item, f"{param}:list item:{list_item}", DTYPES[subtype]
                )
            return
        # convert nulltype strings
        if isinstance(config_item[config_value], str):
            config_item[config_value] = _nulltype_conversion(config_item[config_value])
        type_defence(config_item[config_value], param, dtype)
        # skip validation if Nonetype
        if isinstance(config_item[config_value], type(None)):
            continue
        # validation for each type
        if validation_item["dtype"] == "path":
            _validate_path(config_item[config_value], validation_item)
        if validation_item["dtype"] in ["int", "float"]:
            _validate_numeric(config_item[config_value], param, validation_item)


def validate_config(config: dict) -> None:
    """
    Validate a configuration dictionary based on a validation schema.

    If the 'validate' flag is set to False, no validation is performed.

    Args:
        config (dict): The configuration dictionary to validate.

    Returns:
        None: This function does not return anything.

    Raises:
        TypeError: If the 'validate' flag is not a boolean or the 'path' value is not
            a string.
    """
    # check if validation is required
    validate = config["config_validation"]["validate"]
    # check that the variable is a boolean
    type_defence(validate, "config_validation:validate", bool)
    if not validate:
        return None
    # get the path to the config validation schema and check it is a string
    validation_path = config["config_validation"]["path"]
    type_defence(validation_path, "config_validation:path", str)
    # load the validation schema
    validation_config = safeload_yaml(validation_path)
    # for each item in the validation schema, validate the correponding config item.
    for validation_item in validation_config.keys():
        _validate_config_items(
            config_item=config[validation_item],
            validation_item=validation_config[validation_item],
            item_name=validation_item,
        )


def validate_freezing_config_settings(user_config):
    """Check that correct combination of freezing settings are used."""

    run_first_snapshot_of_results = user_config["global"]["run_first_snapshot_of_results"]
    run_frozen_data = user_config["global"]["run_frozen_data"]
    frozen_snapshot_path = user_config["hdfs_paths"]["frozen_snapshot_path"]
    frozen_data_staged_path = user_config["hdfs_paths"]["frozen_data_staged_path"]
    load_updated_snapshot_for_comparison = user_config["global"]["load_updated_snapshot_for_comparison"]
    secondary_snapshot_path = user_config["hdfs_paths"]["secondary_snapshot_path"]
    run_updates_and_freeze = user_config["global"]["run_updates_and_freeze"]
    freezing_adds_and_amends_path = user_config["hdfs_paths"]["freezing_adds_and_amends_path"]


    if run_first_snapshot_of_results and run_frozen_data:
        raise ValueError(
            "Cannot run first snapshot of results and run frozen data at the same time."
        )

    if run_first_snapshot_of_results:
        if frozen_snapshot_path is None:
            raise ValueError(
                "If running first snapshot of results, a frozen snapshot path must be provided."
            )

    if run_frozen_data:
        if frozen_data_staged_path is None:
            raise ValueError(
                "If running frozen data, a frozen data staged path must be provided."
            )

    if load_updated_snapshot_for_comparison:
        if secondary_snapshot_path is None or frozen_data_staged_path is None:
            raise ValueError(
                "If loading an updated snapshot for comparison, a secondary snapshot path and frozen data staged path must be provided."
            )

    if run_updates_and_freeze:
        if frozen_data_staged_path is None or freezing_adds_and_amends_path is None:
            raise ValueError(
                "If running updates and freezing, a frozen data staged path and freezing adds and amends path must be provided."
            )
