"""Simple defence utilities that validate user inputs."""

import warnings
import os

from typing import Any
from rdsa_utils.typing import PathLike


def type_defence(obj: object, param_nm: str, types: Any, warn: bool = False) -> None:
    """Ensure that a passed parameter is of the correct type.

    Args:
        obj (object): The passed object.
        param_nm (str): The parameter name.
        types (Any): Specified type(s) to match.
        warn (bool, optional): Whether or not to warn in the case that the
                               passed object does not match the given type.
                               Raises an error when False. Defaults to False.

    Raises:
        TypeError: Raised if the passed object is not of the specified type.

    Returns:
        None
    """
    if not isinstance(obj, types):
        msg = f"`{param_nm}` expected {types}. Got {type(obj)}"
        if warn:
            warnings.warn(msg, UserWarning)
        else:
            raise TypeError(msg)
    return None


def validate_file_extension(path: PathLike, ext: str, warn: bool = False):
    """Validate the file extension of a passed path.

    Args:
        path (PathLike): The file path.
        ext (str): The expected file extension.
        warn (bool, optional): _description_. Defaults to False.
    """
    # normalise extension
    if ext[0] != ".":
        ext = f".{ext}"
    # check extension
    found_ext = os.path.splitext(path)
    if found_ext[1] != ext:
        msg = f"Expected file extension {ext} for {path}. Got {found_ext[1]}"
        if warn:
            warnings.warn(msg, UserWarning)
        else:
            raise TypeError(msg)
