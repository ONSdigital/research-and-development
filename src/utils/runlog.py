# """Generate run log output."""

import logging

# import utils.helpers as hlp
from _version import __version__ as version
from datetime import datetime
import numpy as np
import pandas as pd
import configparser

LOGGER = logging.getLogger(__name__)

CONFIG_FILE = "config.yaml"



class RunLog:
    """ Creates a runlog instance for the pipeline.
    """
    def __init__(self, config, version):
        self.config = config
        self.run_id = self.create_run_id()
        self.version = version
        self.logs = []
        self.time_taken = self._get_time_taken()

    def create_run_id(self):
        """Create a unique run_id from the timestamp and a random number.

        Parameters
        ----------
        timestamp : str
            Timestamp of the run.

        Returns
        -------
        str
            Unique run_id.

        """
        # get the time now 
        timestamp = datetime.now()
        run_id = f"{timestamp}_{np.random.randint(0,1000)}"
        return run_id

    # write a function to get all the config settings
    def get_config_settings(self):
        """Get all the config settings.

        Returns
        -------
        dict
            All the config settings.

        """
        config = configparser.ConfigParser()
        config.read(CONFIG_FILE)
        self.config = config

        return self 

    def _get_time_taken(self, time_taken):
        """Get the time taken for the pipeline to run.

        """

        # Not sure how this is going to work until we have the timing wrapper
        # Somehow get the time from the timing wrapper
        # self.time_taken = time_taken
        pass

    def create_runlog_dict(self): 
        """Create a dictionary from the config settings, run_id and version and logs list."""
        self.runlog_dict = {
            "run_id": self.run_id,
            "version": self.version,
            "time_taken": self.time_taken
        }

        # Add all the config settings to the dictionary
        self.runlog_dict.update(self.config)

        return self

    def create_runlog_df(self):
        """Convert runlog_dict to a pandas dataframe."""
        self.runlog_df = pd.DataFrame(self.runlog_dict)
        return self

    def get_runlog_settings(self):
        """Get the runlog settings from the config file."""
        runlog_settings = self.get_config_settings()["runlog_writer"]
        write_csv = runlog_settings["write_csv"]
        write_hdf5 = runlog_settings["write_hdf5"]
        write_sql = runlog_settings["write_sql"]

        return write_csv, write_hdf5, write_sql

    def write_runlog(self):
        """Write the runlog to a file."""
        
        # Get filename from run_id
        filename = self.run_id

        # Get the runlog settings from the config file
        write_csv, write_hdf5, write_sql = self.get_runlog_settings()

        if write_csv:
            # write the runlog to a csv file
            self.runlog_df.to_csv(f"{filename}_runlog.csv")
        if write_hdf5:
            # write the runlog to a hdf5 file
            self.runlog_df.to_hdf(f"{filename}_runlog.hdf5")
        if write_sql:
            # write the runlog to a sql database
            self.runlog_df.to_sql(f"{filename}_runlog.sql")
        
        return None