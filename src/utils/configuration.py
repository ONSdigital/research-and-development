"""Provide functions for parsing and validating configuration files."""

import configparser
from configparser import ExtendedInterpolation
import logging


LOGGER = logging.getLogger(__name__)


class ConfigReader:
    """Read, parse and validate configuration settings.

    Parameters
    ----------
    config_file_path : str
        The full filepath and filename of the .ini config file to load.

    """

    def __init__(self, config_file_path):
        """Initialise, read and validate config settings."""

        self.config_file_path = config_file_path
        self.config = configparser.ConfigParser(interpolation=ExtendedInterpolation())

        try:
            self.config.read_file(open(self.config_file_path))

        except FileNotFoundError as e:
            raise FileNotFoundError(
                f"Could not find configuration file: {self.config_file_path}"
            ) from e

        self.validate_config()

    def validate_config(self):
        """Validate configuration settings."""
        pass

    def log_config_settings(self, name, limit=0):
        """Add all configuration settings in section defined by name to Logger."""
        LOGGER.info("==============================================")
        LOGGER.info(f"Loading {name} configuration settings")

        # Only output the first n items, where n=limit
        key_list = list(self.config[name])
        if limit > 0:
            key_list = key_list[:limit]
        for key in key_list:
            LOGGER.info(f"  {key}: {self.config[name][key]}")
        LOGGER.info("")

    def config_setup(self, name):
        """Return a dictionary of the config settings for name."""
        return self.config[name]

    def config_global(self):
        """Return a dictionary of the global config settings."""
        self.log_config_settings("global")
        return self.config["global"]
