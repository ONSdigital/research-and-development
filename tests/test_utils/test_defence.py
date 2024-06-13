"""Tests for defence.py"""

import pytest

from src.utils.defence import (
    _type_defence,
    _validate_file_extension
)

class TestTypeDefence(object):
    """Tests for _type_defence."""

    def test__type_defence(self):
        """General passing tests for _type_defence."""
        _type_defence(None, 'test', (str, type(None)))
        _type_defence(5, 'test', int)
        _type_defence(True, 'test', (bool, int))
        _type_defence('tester', 'test', str)
    
    def test__type_defence_raises(self):
        """Tests for _type_defence error raises."""
        msg = ".*test_param.* expected .*str.*Got .*int.*"
        with pytest.raises(TypeError, match=msg):
            _type_defence(5, "test_param", str)

    def test__type_defence_warns(self):
        """Tests for _type_defence warnings."""
        msg = ".*test_param.* expected .*bool.*Got .*str.*"
        with pytest.warns(UserWarning, match=msg):
            _type_defence("test", "test_param", bool, warn=True)


class TestValidateFileExtension(object):
    """Tests for _validate_file_extension."""
    
    def test__validate_file_extension(self):
        """General tests for _validate_file_extension."""
        _validate_file_extension("test/test.txt", ".txt")
        _validate_file_extension("test/test.txt", "txt")

    def test__validate_file_extension_raises(self):
        """Tests for _validate_file_extension error raises."""
        msg = "Expected file extension .txt for.*Got .*not_txt.*"
        with pytest.raises(TypeError, match=msg):
            _validate_file_extension("test/test.not_txt", ".txt")

    def test__validate_file_extension_warns(self):
        """Tests for _validate_file_extension error warns."""
        msg = "Expected file extension .txt for.*Got .*not_txt.*"
        with pytest.warns(UserWarning, match=msg):
            _validate_file_extension("test/test.not_txt", ".txt", warn=True)
