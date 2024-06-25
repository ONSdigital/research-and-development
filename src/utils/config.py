"""Simple utils to assist the config."""
from copy import deepcopy
from typing import Union

from src.utils.defence import (
    type_defence, 
    validate_file_extension
)
from src.utils.local_file_mods import safeload_yaml


def merge_configs(config1: dict, config2: dict) -> dict:
    """Merge two config files.

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
            if (
                isinstance(config2[key], dict) and 
                isinstance(config1[key], dict)
            ):
                for subkey in config1[key]:
                    if subkey not in config2[key].keys():
                        config2[key][subkey] = config1[key][subkey]
                    else:
                        raise ValueError(f"Overlapping keys in configs: {key}:{subkey}")
            else:
                raise ValueError(f"Overlapping keys in configs: {key}")

        else:
            config2[key] = config1[key]
    return deepcopy(config2)


def _check_has_schema(_dict: dict) -> bool:
    """Check if an item has a schema.

    Args:
        _dict (dict): The config item.

    Returns:
        bool: Whether config item has required keys.
    """
    expected_keys = [
        "singular",
        "dtype", 
        "accept_nonetype",
    ]
    if set(expected_keys).issubset(set(list(_dict.keys()))):
        return True
    return False


def _validate_path(path: str, config: dict):
    """Validate a passed path (str format) is valid.

    Args:
        path (str): The path to validate.
        item_conf (dict): The config file (for file ext).
    """
    try:
        file_ext = config["filetype"]
    except KeyError:
        file_ext = None
    if file_ext:
        validate_file_extension(path, file_ext)


def _validate_numeric(value: Union[float, int], param_nm: str, config: dict):
    """Valdiate a numerical value fits between a range.

    Args:
        value (Union[float, int]): The numerical value.
        param_nm (str): The parameter name (used for error raises).
        config (dict): The config.

    Raises:
        ValueError: Raised if a number is lesser than the minimum.
        ValueError: Raised if a number is greater than the maximum.
    """
    try:
        max = config["max"]
    except KeyError:
        max = None
    try:
        min = config["min"]
    except KeyError:
        min = None
    if max:
        if value > max:
            raise ValueError(
                f"Config value for {param_nm} ({value}) greater than max ({max})."
            )
    if min:
        if value < min:
            raise ValueError(
                f"Config value for {param_nm} ({value}) less than min ({min})."
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

def _check_items(item: dict, config_item: dict, item_name: str) -> None:
    """Check items of a config to validate them.

    This function recursively checks items in the config and validates them. It
     traverses the tree of the config (levels) until the config validation ymal
    reaches a validation schema. If a valdiation schema is detected, the 
    function will stop recursively going deeper into the tree, and will 
    validate the current level with the parameters passed.

    Args:
        item (dict): The item(s) to check (config validation).
        config_item (dict): The matching config item (actual config). 
        item_name (str): The name of the item (matching in both validation
                         and config)
    """
    DTYPES = {
        "int": int,
        "float": float,
        "str": str,
        "bool": bool,
        "path": str, # passed as string in config
        "list": list
    }
    # recursive check on schema item
    if not _check_has_schema(_dict=item):
        for sub_item in item.keys():
            _check_items(
                item=item[sub_item],
                config_item=config_item[sub_item],
                item_name=sub_item
            )
        return
    # check that both levels aren't the same
    # do validation
    if item["singular"]:
        config_item = {item_name: config_item}
    dtype = DTYPES[item["dtype"]] if "list" not in item["dtype"] else list
    if item["accept_nonetype"]:
        dtype = (dtype, type(None))
    for level in config_item.keys():
        param = f"{item_name}:{level}"
        # parse list datatype
        if "list" in item["dtype"]:
            dtype = item["dtype"]
            type_defence(config_item[level], param, list)
            subtype = dtype.replace("list[", "").replace("]", "")
            for list_item in config_item[level]:
                type_defence(
                    list_item, 
                    f"{param}:list item:{list_item}", 
                    DTYPES[subtype]
                )
            return
        # convert nulltype strings
        if isinstance(config_item[level], str):
            config_item[level] = _nulltype_conversion(config_item[level])
        type_defence(config_item[level], param, dtype)
        # skip validation if Nonetype
        if isinstance(config_item[level], type(None)):
            continue
        # validation for each type
        if item["dtype"] == "path":
            _validate_path(config_item[level], item)
        if item["dtype"] in ["int", "float"]:
            _validate_numeric(config_item[level], param, item)


def validate_config(config: dict) -> None:
    """Validate the information passed to the config file.

    Args:
        config (dict): The config to validate.
    """
    # validate config validation paths
    validate = config["config_validation"]["validate"]
    type_defence(
        validate,
        "config_validation:validate",
        bool
    )
    if not validate:
        return None
    validation_path = config["config_validation"]["path"]
    type_defence(validation_path, "config_validation:path", str)
    # read in config schema
    validation_config = safeload_yaml(validation_path)
    for item in validation_config.keys():
        _check_items(
            item=validation_config[item], 
            config_item=config[item],
            item_name=item
        )
