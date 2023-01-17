"""Function for parsing logging configuration."""

import logging
import logging.config
import yaml
import os


def setup_logger(default_path='config/logging.yaml'):
    """Parse a config yaml file for setting up the configuration.

    Parameters
    ----------
    default_path : str
        The absolute path to the configuration file.

    """
    basic = '%(asctime)s %(levelname)s, %(message)s'
    if os.path.exists(default_path):
        with open(default_path, 'rt') as file:
            try:
                config = yaml.safe_load(file.read())
                logging.config.dictConfig(config)
            except Exception as e:
                logging.basicConfig(filename='logging.log',
                                    filemode='a', format=basic,
                                    level=logging.INFO)
                print(e)
                print('Logging will use default settings')
