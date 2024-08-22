import os
# import csv
from datetime import datetime
from typing import Tuple

import pandas as pd


class RunLog:
    """Creates a runlog instance for the pipeline."""

    def __init__(
        self,
        config,
        version,
        file_exists_func,
        mkdir_func,
        read_csv_func,
        write_csv_func,
    ):
        # config based attrs
        self.config = config
        self.environment = config["global"]["network_or_hdfs"]
        self.logs_folder = config[f"{self.environment}_paths"]["logs_foldername"]
        self.log_filenames = config["log_filenames"]
        # user information
        self.user = self._generate_username()
        # attrs containing callables
        self.file_exists_func = file_exists_func
        self.mkdir_func = mkdir_func
        self.read_csv_func = read_csv_func
        self.write_func = write_csv_func
        self.write_csv_func = write_csv_func
        # pipeline information
        self.run_id = self._create_run_id()
        self.version = version
        # logs
        self.logs = []
        self.timestamp = datetime.now().strftime("%d/%m/%Y-%H:%M:%S")

    def _create_folder(self):
        """Create the folder for the runlog if it doesn't exist."""
        if not self.file_exists_func(self.logs_folder):
            self.mkdir_func(self.logs_folder)

    def _generate_username(self):
        """Record the username of the user running the pipeline using os package"""
        # Use the Hadoop Username to record user
        self.context = os.getenv("HADOOP_USER_NAME")
        if self.context is None:  # Running local Python yields None here
            self.context = "local_dev_run"
        return self.context

    def _create_run_id(self):
        """Create a unique run_id from the previous iteration"""
        # Import name of main log file
        main_path = self.log_filenames["main"]
        mainfile = os.path.join(self.logs_folder, main_path)
        latest_id = 0
        # If the file exists, read it using the read_csv function
        if self.file_exists_func(mainfile):
            runfile = self.read_csv_func(mainfile)
            # Check if the dataframe has at least one data row
            if len(runfile):
                latest_id = max(runfile.run_id)
                
        # increment the latest id by 1
        run_id = latest_id + 1
        return run_id

    def _record_time_taken(self):
        """Get the time taken for the pipeline to run (in seconds).

        This is for the total pipeline run time, not the time taken for each step.

        """
        start_time = datetime.strptime(self.timestamp, "%d/%m/%Y-%H:%M:%S")
        self.time_taken = (datetime.now() - start_time).total_seconds()
        return self.time_taken

    def _retrieve_pipeline_logs(self):
        """
        Get all of the logs from the pipeline run and append them to self.saved_logs df.
        """
        with open("logs/main.log", "r") as f:
            # Split logs by line
            lines = f.read().splitlines()

        self.runids = {"run_id": self.run_id}
        self.users = {"user": self.user}
        # Add run_id and user to logs
        for line in lines:
            self.logs.append(line.split(" - "))
            self.runids.update({"run_id": self.run_id})
            self.users.update({"user": self.user})
        self.run_logs_df = pd.DataFrame(
            self.logs, columns=["timestamp", "module", "function", "message"]
        )
        self.run_logs_df.insert(0, "run_id", self.runids["run_id"])
        self.run_logs_df.insert(1, "user", self.users["user"])

        return self.run_logs_df

    def _create_mainlog_df(self) -> pd.DataFrame:
        """Create the new row for the main log."""
        self.mainlog_df = pd.DataFrame(
            {
                "run_id": [self.run_id],
                "user": [self.user],
                "status": ["FAILED"],
                "timestamp": [self.timestamp],
                "version": [self.version],
                "time_taken": [0],
            }
        )

        return self.mainlog_df

    def _get_runlog_settings(self) -> Tuple[bool, bool, bool]:
        """Get the runlog settings from the config file."""
        runlog_settings = self.config["runlog_writer"]
        write_csv_setting = runlog_settings["write_csv"]
        write_hdf5_setting = runlog_settings["write_hdf5"]
        write_sql_setting = runlog_settings["write_sql"]

        return write_csv_setting, write_hdf5_setting, write_sql_setting

    def log_csv_creator(self, filepath: str, columns: list) -> None:
        """Creates a csv file in DAP with user-defined headers if it doesn't
        exist. There may be no runlog if the pipeline is running for the first
        time, run_id would be 1. If so, create a CSV file with column names and
        no data rows, so that new entries can be inserted in it.

        Args:
            filename (string): Example: "name_of_file.csv"
            columns (list): Example: ["a","b","c","d"]

        Returns: None
        """
        # Check if the runolg file exists. 
        if not self.file_exists_func(filepath):
            # Create an empty dataframe with column names
            df = pd.DataFrame(columns=columns)
            # Create new csv file in specified folder
            self.write_csv_func(filepath, df)
        return None

    def create_runlog_files(self):
        """Creates csv files with column names if they don't already exist."""

        main_columns = [
            "run_id",
            "user",
            "status",
            "timestamp",
            "version",
            "time_taken",
        ]
        file_name = self.log_filenames["main"]
        file_path = str(os.path.join(self.logs_folder, file_name))
        self.log_csv_creator(file_path, main_columns)

        config_columns = list(self._retrieve_config_log().columns)
        file_name = self.log_filenames["configs"]
        file_path = str(os.path.join(self.logs_folder, file_name))
        self.log_csv_creator(file_path, config_columns)

        log_columns = ["run_id", "user", "timestamp", "module", "function", "message"]
        file_name = self.log_filenames["logs"]
        file_path = str(os.path.join(self.logs_folder, file_name))
        self.log_csv_creator(file_path, log_columns)

        return None

    def _retrieve_config_log(self) -> pd.DataFrame:
        """Gets the configs settings for each run of the pipeline"""
        # Convert the YAML data to a Pandas DataFrame
        dct = {k: [v] for k, v in self.config.items()}
        self.ndct = {}
        # Use all the 2nd level yaml keys as headers
        for i in dct.keys():
            nrow = {k: [v] for k, v in dct[i][0].items()}
            self.ndct.update(nrow)
        config_log_df = pd.DataFrame(self.ndct)
        # Add run_id and user to configs
        config_log_df.insert(0, "run_id", self.run_id)
        config_log_df.insert(1, "user", self.user)
        self.config_log_df = config_log_df
        return self.config_log_df

    def _read_log(
        self,
        logfile_name: str,
    ) -> pd.DataFrame:
        """Read logs from a .csv file.

        Args:
            logfile_name (str): The name of the file to read.

        Returns:
            pd.DataFrame: A dataframe containing logs.
        """
        write_csv, write_hdf5, write_sql = self._get_runlog_settings()
        if write_csv:
            # write the runlog to a csv file
            file_path = str(os.path.join(self.logs_folder, logfile_name))
            return self.read_csv_func(file_path)
        elif write_hdf5:
            # write the runlog to a hdf5 file
            logfile_name = f"{os.path.splitext(logfile_name)[0]}.hdf"
            return pd.read_hdf(logfile_name)
        elif write_sql:
            # write the runlog to a sql database
            logfile_name = f"{os.path.splitext(logfile_name)[0]}.sql"
            return pd.read_sql(logfile_name)

    def _write_log(
        self, logfile_name: str, logs_df: pd.DataFrame, update: bool = False
    ) -> None:
        """Write logs (in df format) to a file.

        Args:
            logfile_name (str): The name of the file to write to,
            logs_df (pd.DataFrame): The dataframe to append to the logs file.
            update (bool, optional): Whether or not to write the dataframe as the logs,
                or to append to current logs. Defaults to False.
        """
        # Get the runlog settings from the config file
        write_csv, write_hdf5, write_sql = self._get_runlog_settings()
        if write_csv:
            # write the runlog to a csv file
            file_path = str(os.path.join(self.logs_folder, logfile_name))
            if update:
                self.write_func(file_path, logs_df)
            else:
                df = self.read_csv_func(file_path)
                df = df.append(logs_df)
                self.write_func(file_path, df)
        if write_hdf5:
            # write the runlog to a hdf5 file
            logfile_name = f"{os.path.splitext(logfile_name)[0]}.hdf"
            if update:
                logs_df.to_hdf(logfile_name, mode="a", index=False, header=False)
            else:
                df = pd.read_hdf(logfile_name)
                df = df.append(self.logs_df)
                df.to_hdf(logfile_name, mode="a", index=False, header=False)
        if write_sql:
            # write the runlog to a sql database
            logfile_name = f"{os.path.splitext(logfile_name)[0]}.sql"
            if update:
                logs_df.to_sql(logfile_name, mode="a", index=False, header=False)
            else:
                df = pd.read_sql(logfile_name)
                df = df.append(logs_df)
                df.to_sql(logfile_name, mode="a", index=False, header=False)

    def write_runlog(self) -> None:
        """Write the logs from the pipeline run to file."""
        logs = self._retrieve_pipeline_logs()
        self._write_log(self.log_filenames["logs"], logs)
        return None

    def write_config_log(self) -> None:
        """Write the config log to file."""
        logs = self._retrieve_config_log()
        self._write_log(self.log_filenames["configs"], logs)
        return None

    def write_mainlog(self) -> None:
        """Write the mainlog to file."""
        self._create_mainlog_df()
        self._write_log(self.log_filenames["main"], self.mainlog_df)
        return None

    def mark_mainlog_passed(self):
        """Mark the most recent run as passed in the main log."""
        self._record_time_taken()
        mainlog_df = self._read_log(self.log_filenames["main"])
        (
            mainlog_df.loc[
                mainlog_df[mainlog_df.run_id == self.run_id].index[0], "status"
            ]
        ) = "PASSED"
        (
            mainlog_df.loc[
                mainlog_df[mainlog_df.run_id == self.run_id].index[0], "time_taken"
            ]
        ) = self.time_taken
        self._write_log(self.log_filenames["main"], mainlog_df, update=True)
