# """Generate run log output."""

import logging

# import utils.helpers as hlp

from datetime import datetime
import pandas as pd
import os

LOGGER = logging.getLogger(__name__)


class RunLog:
    """Creates a runlog instance for the pipeline."""

    def __init__(self, config, version):
        self.config = config
        self.run_id = self._create_run_id()
        self.version = version
        self.logs = []
        self.timestamp = self._generate_time()

    def _create_run_id(self):
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
        files = os.listdir("logs/")
        log_files = [f for f in files if ".log" in f]
        if log_files:
            id_list = []
            for i in range(len(log_files)):
                ids = log_files[i].split("_")[1]
                ids = ids.split(".")[0]
                id_list.append(int(ids))
            latest_id = max(id_list)
        else:
            latest_id = 0
        run_id = latest_id + 1
        return run_id

    def _record_time_taken(self, time_taken):
        """Get the time taken for the pipeline to run.

        This is for the total pipeline run time, not the time taken for each step.

        """

        self.time_taken = str(time_taken)

        return self.time_taken

    def retrieve_pipeline_logs(self):
        """
        Get all of the logs from the pipeline (from the log files)
        and append them to self.logs list

        """

        files = os.listdir("logs/")
        log_files = [f for f in files if f"{self.run_id}.log" in f]
        for i in range(len(log_files)):
            f = open(os.path.join("logs/", log_files[i]), "r")
            lines = f.read().splitlines()
            self.logs.append(lines)

        return self

    def _generate_time(self):
        timestamp = datetime.now().strftime("%d/%m/%Y-%H:%M:%S")
        return timestamp

    def _create_main_dict(self):
        """Create unique dictionaries for runlogs, configs
        and loggers with run_id as identifier"""

        self.runlog_main_dict = {
            "run_id": self.run_id,
            "timestamp": self.timestamp,
            "version": self.version,
            "run_time": self.time_taken,
        }

        return self

    def _create_config_dict(self):

        self.runlog_configs_dict = {
            "run_id": self.run_id,
            "configs": self.config,
        }

        return self

    def _create_logs_dict(self):

        self.runlog_logs_dict = {
            "run_id": self.run_id,
            "logs": self.logs,
        }

        return self

    def _create_runlog_dfs(self):
        """Convert dictionaries to pandas dataframes."""

        self.runlog_main_dict = pd.DataFrame(self.runlog_main_dict)
        self.runlog_configs_dict = pd.DataFrame(self.runlog_configs_dict)
        self.runlog_logs_dict = pd.DataFrame(self.runlog_logs_dict)

        return self

    def _get_runlog_settings(self):

        """Get the runlog settings from the config file."""
        runlog_settings = self.config["runlog_writer"]
        write_csv = runlog_settings["write_csv"]
        write_hdf5 = runlog_settings["write_hdf5"]
        write_sql = runlog_settings["write_sql"]

        return write_csv, write_hdf5, write_sql

    def _write_runlog(self):
        """Write the runlog to a file."""

        # Get filename from run_id
        filename = self.run_id

        # Get the runlog settings from the config file
        write_csv, write_hdf5, write_sql = self._get_runlog_settings()

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
