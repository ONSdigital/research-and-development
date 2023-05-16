from datetime import datetime
import pandas as pd
import os
from src.utils.helpers import Config_settings, csv_creator
import yaml


class RunLog:
    """Creates a runlog instance for the pipeline."""

    def __init__(self, config, version):
        self.config = config
        self.run_id = self._create_run_id()
        self.version = version
        self.logs = []
        self.timestamp = self._generate_time()

    def _create_run_id(self):
        """Create a unique run_id from the previous iteration"""
        fp = "main_runlog.csv"
        if os.path.isfile(fp):
            files = pd.read_csv(fp)
            latest_id = int(max(files.run_id))
        else:
            latest_id = 0
        run_id = latest_id + 1
        return run_id

    def _record_time_taken(self, time_taken):
        """Get the time taken for the pipeline to run.

        This is for the total pipeline run time, not the time taken for each step.

        """

        self.time_taken = time_taken

        return self.time_taken

    def retrieve_pipeline_logs(self):
        """
        Get all of the logs from the pipeline run
        and append them to self.logs list

        """
        f = open("logs/main.log", "r")
        lines = f.read().splitlines()
        self.runids = {"run_id": self.run_id}
        for line in lines:
            self.logs.append(line.split(" - "))
            self.runids.update({"run_id": self.run_id})
        self.saved_logs = pd.DataFrame(
            self.logs, columns=["timestamp", "module", "function", "message"]
        )
        self.saved_logs.insert(0, "run_id", self.runids["run_id"])
        return self

    def retrieve_configs(self):
        with open("src/developer_config.yaml", "r") as file:
            self.configdata = yaml.load(file, Loader=yaml.FullLoader)
        # Convert the YAML data to a Pandas DataFrame
        dct = {k: [v] for k, v in self.configdata.items()}
        self.ndct = {}
        for i in dct.keys():
            nrow = {k: [v] for k, v in dct[i][0].items()}
            self.ndct.update(nrow)
        self.configdf = pd.DataFrame(self.ndct)
        self.configdf.insert(0, "run_id", self.runids["run_id"])
        return self

    def _generate_time(self):
        """Generate unqiue timestamp for when each run"""
        timestamp = datetime.now().strftime("%d/%m/%Y-%H:%M:%S")
        return timestamp

    def _create_runlog_dicts(self):
        """Create unique dictionaries for runlogs, configs
        and loggers with run_id as identifier"""

        self.runlog_main_dict = {
            "run_id": self.run_id,
            "timestamp": self.timestamp,
            "version": self.version,
            "time_taken": self.time_taken,
        }

        return self

    def _create_runlog_dfs(self):
        """Convert dictionaries to pandas dataframes."""
        self.runlog_main_df = pd.DataFrame(
            [self.runlog_main_dict],
            columns=["run_id", "timestamp", "version", "time_taken"],
        )

        self.runlog_configs_df = self.configdf

        self.runlog_logs_df = self.saved_logs

        return self

    def _get_runlog_settings(self):

        """Get the runlog settings from the config file."""
        write_csv = self.config["runlog_writer"]["write_csv"]
        write_hdf5 = self.config["runlog_writer"]["write_hdf5"]
        write_sql = self.config["runlog_writer"]["write_sql"]

        return write_csv, write_hdf5, write_sql

    def create_runlog_files(self):
        """Creates csv files with column names
        if they don't already exist.
        """
        conf_obj = Config_settings()
        config = conf_obj.config_dict

        main_columns = ["run_id", "timestamp", "version", "duration"]
        file_name = config["csv_filenames"]["main"]
        csv_creator(file_name, main_columns)

        config_columns = list(self.configdf.columns.values)
        file_name = config["csv_filenames"]["configs"]
        csv_creator(file_name, config_columns)

        log_columns = ["run_id", "timestamp", "module", "function", "message"]
        file_name = config["csv_filenames"]["logs"]
        csv_creator(file_name, log_columns)

        return None

    def _write_runlog(self):
        """Write the runlog to a file specified in the config."""

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
