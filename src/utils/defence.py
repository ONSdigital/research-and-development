"""Simple defence utilities that validate user inputs."""
import pathlib
import warnings
import os
from typing import Union

def _type_defence(
        obj: object, 
        param_nm: str,
        types: Union[object, tuple], 
        warn: bool=False
    ) -> None:
    """Ensure that a passed parameter is of the correct type.

    Args:
        obj (object): The passed object.
        param_nm (str): The parameter name.
        types (Union[object, tuple]): Specified type(s) to match.
        warn (bool, optional): Whether or not to warn in the case that the 
                               passed object does not match the given type.
                               Raises when False. Defaults to False.

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
            raise TypeError(
                msg
            )
    return None


def _validate_str_is_path(path: str, param_nm: str, warn: bool=False):
    """Validate the a path passed in string form is valid.

    Args:
        path (str): The path.
        param_nm (str): The parameter name (for error raising).
        warn (bool, optional): Whether or not to warn. Defaults to False.
    """
    pass


def _validate_file_extension(
        path: Union[pathlib.Path, str],
        ext: str, 
        warn: bool=False
    ):
    """Validate the file extension of a passed path.

    Args:
        path (Union[pathlib.Path, str]): The file path.
        ext (str): The expected file extension.
        warn (bool, optional): _description_. Defaults to False.
    """
    # normalise extension
    if ext[0] != ".":
        ext = f".{ext}"
    # check extension
    found_ext = os.path.splitext(path)
    if found_ext != ext:
        msg = f"Expected file extension {ext} for {path}. Got {found_ext}"
        if warn:
            warnings.warn(msg, UserWarning)
        else:
            raise TypeError(
                msg
            )
    