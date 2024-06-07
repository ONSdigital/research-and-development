"""Simple utils to assist the config."""
import pathlib
import os
from copy import deepcopy
from typing import Union

import yaml

from src.utils.defence import (
    _type_defence, 
    _validate_str_is_path, 
    _validate_file_extension
)

def safeload_yaml(path: Union[str, pathlib.Path]) -> dict:
    """Load a .yaml file from a path.

    Args:
        path (Union[str, pathlib.Path]): The path to load the .yaml file from.

    Raises:
        FileNotFoundError: Raised if there is no file at the given path.
        TypeError: Raised if the file does not have the .yaml extension.

    Returns:
        dict: The loaded yaml file as as dictionary.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Attempted to load yaml at: {path}. File "
            "does not exist."
        )
    ext = os.path.splitext(path)[1]
    if ext != ".yaml":
        raise TypeError(
            f"Expected a .yaml file. Got {ext}"
        )
    with open(path, "r") as f:
        yaml_dict = yaml.safe_load(f)
    return yaml_dict


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


def _check_has_schema(value: dict) -> bool:
    """Check if an item has a schema.

    Args:
        value (dict): The item.

    Returns:
        bool: Whether or not the item has a schema.
    """
    expected_keys = [
        "top_level", 
        "bottom_level", 
        "dtype", 
        "accept_nonetype",
    ]
    if sorted(expected_keys) == sorted(list(value.keys())[:4]):
        return True
    return False


def _validate_path(path: str, param_nm: str, config: dict):
    """Validate a passed path (str format) is valid.

    Args:
        path (str): The path to validate.
        param_nm (str): The param name (for error raises).
        item_conf (dict): The config file (for file ext).
    """
    _validate_str_is_path(path, param_nm)
    try:
        file_ext = config["filetype"]
    except KeyError:
        file_ext = None
    if file_ext:
        _validate_file_extension(path, file_ext)


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
                f"Config value ({value}) {param_nm} greater than max ({max})."
            )
    if min:
        if value < min:
            raise ValueError(
                f"Config value ({value}) {param_nm} less than min ({min})."
            )


def _nulltype_conversion(value: str) -> Union[str, None]:
    """Account for user disparities in the form of NoneType they pass.

    Args:
        value (str): The value passed by a user.

    Returns:
        Union[str, None]: The altered value.
    """
    _type_defence(value, "value", str)
    if value.lower() in ["", "null", "none"]:
        return None
    return value


def _check_items(item: dict, config_item: dict, item_name: str) -> None:
    """Check items of a config to validate them.

    Args:
        item (dict): The item(s) to check.
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
    if not _check_has_schema(value=item):
        for sub_item in item.keys():
            _check_items(
                item=item[sub_item],
                config_item=config_item[sub_item],
                item_name=sub_item
            )
        return
    # check that both levels aren't the same
    if item["top_level"] == item["bottom_level"]:
        raise ValueError(
            "Conflicting config. top_level == botom_level"
        )
    # do validation
    if item["bottom_level"]:
        config_item = {item_name: config_item}
    dtype = DTYPES[item["dtype"]] if "list" not in item["dtype"] else list
    if item["accept_nonetype"]:
        dtype = (dtype, type(None))
    for level in config_item.keys():
        param = f"{item_name}:{level}"
        # parse list datatype
        if "list" in item["dtype"]:
            dtype = item["dtype"]
            _type_defence(config_item[level], param, list)
            subtype = dtype.replace("list[", "").replace("]", "")
            for list_item in config_item[level]:
                _type_defence(
                    list_item, 
                    f"{param}:list item:{list_item}", 
                    DTYPES[subtype]
                )
            return
        # convert nulltype strings
        if isinstance(config_item[level], str):
            config_item[level] = _nulltype_conversion(config_item[level])
        _type_defence(config_item[level], param, dtype)
        # skip validation if Nonetype
        if isinstance(config_item[level], type(None)):
            continue
        # validation for each type
        if item["dtype"] == "path":
            _validate_path(config_item[level], param, item)
        if item["dtype"] in ["int", "float"]:
            _validate_numeric(config_item[level], param, item)


def validate_config(config: dict) -> None:
    """Validate the information passed to the config file.

    Args:
        config (dict): The config to validate.
    """
    # validate config validation paths
    validate = config["config_validation"]["validate"]
    _type_defence(
        validate,
        "config_validation:validate",
        bool
    )
    if not validate:
        return None
    validation_path = config["config_validation"]["path"]
    _type_defence(validation_path, "config_validation:path", str)
    _validate_str_is_path(validation_path, "config_validation:path")
    # read in config schema
    validation_config = safeload_yaml(validation_path)
    for item in validation_config.keys():
        _check_items(
            item=validation_config[item], 
            config_item=config[item],
            item_name=item
        )
