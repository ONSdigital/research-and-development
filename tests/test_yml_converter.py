"""Create a test suite for the yml conversion module."""

import pytest


def test_yml_dependencies():

    # Arrange
    from src.utils.yml_converter import yml_dependencies

    # Act: use pytest to assert the result
    test = yml_dependencies()

    # Assert
    assert type(test) == list

    pytest.raises(TypeError, yml_dependencies, 1)


def test_yml_conda_dependencies():

    # Arrange
    from src.utils.yml_converter import yml_conda_dependencies

    # Act: use pytest to assert the result
    test = yml_conda_dependencies()

    # Assert
    assert type(test) == list


def test_yml_pip_dependencies():

    # Arrange
    from src.utils.yml_converter import yml_pip_dependencies

    # Act: use pytest to assert the result
    test = yml_pip_dependencies()

    # Assert
    assert type(test) == list


def test_deps_combnd():

    # Arrange
    from src.utils.yml_converter import deps_combnd

    # Act: use pytest to assert the result
    test = deps_combnd()

    # Assert
    assert type(test) == list


def test_req_check():

    # Arrange
    from src.utils.yml_converter import req_check

    # Act: use pytest to assert the result
    test = req_check()

    # Assert
    assert type(test) == bool


def test_req_create():

    # Arrange
    from src.utils.yml_converter import req_create

    # Act: use pytest to assert the result
    test = req_create()

    # Assert
    assert type(test) == bool


def test_req_compare():

    # Arrange
    from src.utils.yml_converter import req_compare

    # Act: use pytest to assert the result
    test = req_compare()

    # Assert
    assert type(test) == list


def test_req_write():

    # Arrange
    from src.utils.yml_converter import req_write

    # Act: use pytest to assert the result
    test = req_write()

    # Assert
    assert type(test) == str
