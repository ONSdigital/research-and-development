"""Define helper functions that wrap regularly-used functions."""

import yaml


class Config_settings:
    """Get the config settings from the config file."""
    def __init__(self):
        self.config_file = "src/config.yaml"
        self.config_dict = self.get_config_settings()

    def get_config_settings(self):
        """Get the config settings from the config file."""
        with open(self.config_file, "r") as file:
            config = yaml.safe_load(file)
        return config

