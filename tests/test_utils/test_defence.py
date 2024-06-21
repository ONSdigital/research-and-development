"""Tests for defence.py"""

import pytest

from src.utils.defence import (
    type_defence,
    validate_file_extension
)

class TestTypeDefence(object):
    """Tests for type_defence."""

    def test_type_defence(self):
        """General passing tests for type_defence."""
        type_defence(None, 'test', (str, type(None)))
        type_defence(5, 'test', int)
        type_defence(True, 'test', (bool, int))
        type_defence('tester', 'test', str)
    
    def test_type_defence_raises(self):
        """Tests for type_defence error raises."""
        msg = ".*test_param.* expected .*str.*Got .*int.*"
        with pytest.raises(TypeError, match=msg):
            type_defence(5, "test_param", str)

    def test_type_defence_warns(self):
        """Tests for type_defence warnings."""
        msg = ".*test_param.* expected .*bool.*Got .*str.*"
        with pytest.warns(UserWarning, match=msg):
            type_defence("test", "test_param", bool, warn=True)


class TestValidateFileExtension(object):
    """Tests for validate_file_extension."""
    
    def test_validate_file_extension(self):
        """General tests for validate_file_extension."""
        validate_file_extension("test/test.txt", ".txt")
        validate_file_extension("test/test.txt", "txt")

    def test_validate_file_extension_raises(self):
        """Tests for validate_file_extension error raises."""
        msg = "Expected file extension .txt for.*Got .*not_txt.*"
        with pytest.raises(TypeError, match=msg):
            validate_file_extension("test/test.not_txt", ".txt")

    def test_validate_file_extension_warns(self):
        """Tests for validate_file_extension error warns."""
        msg = "Expected file extension .txt for.*Got .*not_txt.*"
        with pytest.warns(UserWarning, match=msg):
            validate_file_extension("test/test.not_txt", ".txt", warn=True)
