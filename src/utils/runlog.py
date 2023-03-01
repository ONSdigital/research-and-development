import logging
from datetime import datetime
import pandas as pd
import os

class RunLog:
    """Creates a runlog instance for the pipeline."""

    def __init__(self, config, version):
        self.config = config
        self.run_id = self._create_run_id()
        self.version = version
        self.logs = []
        self.timestamp = self._generate_time()

    def _create_run_id(self):
        """Create a unique run_id from the previous iteration

        """
        fp = "main_runlog.csv"
        if os.path.isfile(fp):
            files = pd.read_csv(fp)
            latest_id = max(files.run_id)
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
        Get all of the logs from the pipeline run
        and append them to self.logs list

        """
        f = open("logs/main.log", "r")
        lines = f.read().splitlines()
        self.logs.append(lines)
        
        return self

    def _generate_time(self):
        """ Generate unqiue timestamp for when each run
        """
        timestamp = datetime.now().strftime("%d/%m/%Y-%H:%M:%S")
        return timestamp

    def _create_runlog_dicts(self):
        """Create unique dictionaries for runlogs, configs
        and loggers with run_id as identifier"""

        self.runlog_main_dict = {
            "run_id": self.run_id,
            "timestamp": self.timestamp,
            "version": self.version,
            "duration": self.time_taken,
        }

        self.runlog_configs_dict = {
            "run_id": self.run_id,
            "configs": self.config,
        }
        
        self.runlog_logs_dict = {
            "run_id": self.run_id,
            "logs": self.logs,
        }

        return self

    def _create_runlog_dfs(self):
        """Convert dictionaries to pandas dataframes."""

        self.runlog_main_df = pd.DataFrame(
           [self.runlog_main_dict],columns=['run_id','timestamp','version','duration']
        )
        self.runlog_configs_df = pd.DataFrame(
            [self.runlog_configs_dict],columns=['run_id','configs']
        )
        self.runlog_logs_df = pd.DataFrame(
            [self.runlog_logs_dict],columns=['run_id','logs']
        )

        return self

    def _get_runlog_settings(self):

        """Get the runlog settings from the config file."""
        runlog_settings = self.config["runlog_writer"]
        write_csv = runlog_settings["write_csv"]
        write_hdf5 = runlog_settings["write_hdf5"]
        write_sql = runlog_settings["write_sql"]

        return write_csv, write_hdf5, write_sql

    def create_runlog_files(self):

        main_columns = ["run_id", "timestamp", "version", "duration"]
        fn = "main_runlog.csv"
        if not os.path.exists(fn):
            with open(fn, mode="w", encoding="utf-8") as f:
                f.write(",".join(main_columns) + "\n")

        config_columns = ["run_id", "configs"]
        fn = "configs_runlog.csv"
        if not os.path.exists(fn):
            with open(fn, mode="w", encoding="utf-8") as f:
                f.write(",".join(config_columns) + "\n")

        log_columns = ["run_id", "logs"]
        fn = "logs_runlog.csv"
        if not os.path.exists(fn):
            with open(fn, mode="w", encoding="utf-8") as f:
                f.write(",".join(log_columns) + "\n")

        return None

    def _write_runlog(self):
        """Write the runlog to a file."""

        # Get the runlog settings from the config file
        write_csv, write_hdf5, write_sql = self._get_runlog_settings()

        if write_csv:
            # write the runlog to a csv file
            self.runlog_main_df.to_csv(
                "main_runlog.csv", mode="a", index=False, header=False
            )
            self.runlog_configs_df.to_csv(
                "configs_runlog.csv", mode="a", index=False, header=False
            )
            self.runlog_logs_df.to_csv(
                "logs_runlog.csv", mode="a", index=False, header=False
            )
        if write_hdf5:
            # write the runlog to a hdf5 file
            self.runlog_main_df.to_hdf(
                "main_runlog.hdf", mode="a", index=False, header=False
            )
            self.runlog_configs_df.to_hdf(
                "configs_runlog.hdf", mode="a", index=False, header=False
            )
            self.runlog_logs_df.to_hdf(
                "logs_runlog.hdf", mode="a", index=False, header=False
            )
        if write_sql:
            # write the runlog to a sql database
            self.runlog_main_df.to_sql(
                "main_runlog.sql", mode="a", index=False, header=False
            )
            self.runlog_configs_df.to_sql(
                "configs_runlog.sql", mode="a", index=False, header=False
            )
            self.runlog_logs_df.to_sql(
                "logs_runlog.sql", mode="a", index=False, header=False
            )

        return None
