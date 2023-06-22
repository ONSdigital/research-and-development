from datetime import datetime
import pandas as pd
import os
from src.utils.helpers import Config_settings
import csv
import yaml

try:
    import pydoop.hdfs as hdfs
    from src.utils.hdfs_mods import read_hdfs_csv, write_hdfs_csv
    HDFS_AVAILABLE = True
except ImportError:
    HDFS_AVAILABLE = False

conf_obj = Config_settings()
config = conf_obj.config_dict
csv_filenames = config["csv_filenames"]


class RunLog:
    """Creates a runlog instance for the pipeline."""

    def __init__(self, config, version, file_open_func, file_exists_func, mkdir_func):
        self.user = self._generate_username()
        self.config = config
        self.file_open_func = file_open_func
        self.file_exists_func = file_exists_func
        self.mkdir_func = mkdir_func
        self.run_id = self._create_run_id()
        self.version = version
        self.project = config["paths"]["logs_foldername"] 
        self.logs = []
        self.timestamp = self._generate_time()
        self._create_folder()

    def _create_folder(self):
        """Create the folder for the runlog if it doesn't exist."""
         # Taken from config file
        self.main_path = f"logs/{self.run_id}"
        # create the folder if it doesn't exist
        if not self.file_exists_func(self.main_path):
            self.mkdir(self.main_path) 

    def _generate_username(self):
        """Record the username of the user running the pipeline
        using os package"""
        # Use the Hadoop Username to record user
        try:
            self.context = os.getenv("HADOOP_USER_NAME")
        except:
            self.context = "local_dev_run"
        return self.context

    def _create_run_id(self):
        """Create a unique run_id from the previous iteration"""
        # Import name of main log file
        runid_path = csv_filenames["main"]
        mainfile = f"{self.main_path}/{runid_path}"
        latest_id = 0

        # Check if file exists using the open function provided
        if self.file_open_func and os.path.isfile(mainfile):
            with self.file_open_func(mainfile, "r") as file:
                runfile = pd.read_csv(file)
                latest_id = max(runfile.run_id)

        # increment the latest id by 1
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
        and append them to self.saved_logs df.
        """
        f = open("logs/main.log", "r")
        # Split logs by line
        lines = f.read().splitlines()
        self.runids = {"run_id": self.run_id}
        self.users = {"user": self.user}
        # Add run_id and user to logs
        for line in lines:
            self.logs.append(line.split(" - "))
            self.runids.update({"run_id": self.run_id})
            self.users.update({"user": self.user})
        self.saved_logs = pd.DataFrame(
            self.logs, columns=["timestamp", "module", "function", "message"]
        )
        self.saved_logs.insert(0, "run_id", self.runids["run_id"])
        self.saved_logs.insert(1, "user", self.users["user"])

        return self

    def retrieve_configs(self):
        """Gets the configs settings for each run of the pipeline"""
        with open("src/developer_config.yaml", "r") as file:
            self.configdata = yaml.load(file, Loader=yaml.FullLoader)
        # Convert the YAML data to a Pandas DataFrame
        dct = {k: [v] for k, v in self.configdata.items()}
        self.ndct = {}
        # Use all the 2nd level yaml keys as headers
        for i in dct.keys():
            nrow = {k: [v] for k, v in dct[i][0].items()}
            self.ndct.update(nrow)
        self.configdf = pd.DataFrame(self.ndct)
        # Add run_id and user to configs
        self.configdf.insert(0, "run_id", self.runids["run_id"])
        self.configdf.insert(1, "user", self.users["user"])

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
            "user": self.user,
            "timestamp": self.timestamp,
            "version": self.version,
            "time_taken": self.time_taken,
        }

        return self

    def _create_runlog_dfs(self):
        """Convert dictionaries to pandas dataframes."""
        self.runlog_main_df = pd.DataFrame(
            [self.runlog_main_dict],
            columns=["run_id", "user", "timestamp", "version", "time_taken"],
        )
        # These dfs were created earlier. Renaming for continuity
        self.runlog_configs_df = self.configdf

        self.runlog_logs_df = self.saved_logs

        return self

    def _get_runlog_settings(self):

        """Get the runlog settings from the config file."""
        runlog_settings = self.config["runlog_writer"]
        write_csv = runlog_settings["write_csv"]
        write_hdf5 = runlog_settings["write_hdf5"]
        write_sql = runlog_settings["write_sql"]

        return write_csv, write_hdf5, write_sql

    def log_csv_creator(self, filepath: str, columns: list):
        """Creates a csv file in DAP with user
        defined headers if it doesn't exist.
        Args:
            filename (string): Example: "name_of_file.csv"
            columns (list): Example: ["a","b","c","d"]
        """

        # Check if the file exists
        if not self.file_exists_func(filepath):
            # open the file in write mode inside Hadoop context
            with self.file_open_func(filepath, "wt") as file:
                # Create new csv file in specified folder
                writer = csv.writer(file)
                # Add the headers to the new csv
                writer.writerow(columns)

        return None

    def create_runlog_files(self):
        """Creates csv files with column names
        if they don't already exist.
        """

        main_columns = ["run_id", "user", "timestamp", "version", "time_taken"]
        file_name = csv_filenames["main"]
        file_path = f"{self.main_path}/{file_name}"
        self.log_csv_creator(file_path, main_columns)

        config_columns = list(self.configdf.columns.values)
        file_name = csv_filenames["configs"]
        file_path = f"{self.main_path}/{file_name}"
        self.log_csv_creator(file_path, config_columns)

        log_columns = ["run_id", "user", "timestamp", "module", "function", "message"]
        file_name = csv_filenames["logs"]
        file_path = f"{self.main_path}/{file_name}"
        self.log_csv_creator(file_path, log_columns)

        return None

    def _write_runlog(self):
        """Write the runlog to a file specified in the config."""

        # Get the runlog settings from the config file
        write_csv, write_hdf5, write_sql = self._get_runlog_settings()

        if write_csv:
            # write the runlog to a csv file

            file_name = csv_filenames["main"]
            file_path = f"{self.main_path}/{file_name}"
            df = read_hdfs_csv(file_path)
            newdf = df.append(self.runlog_main_df)
            write_hdfs_csv(file_path, newdf)

            file_name = csv_filenames["configs"]
            file_path = f"{self.main_path}/{file_name}"
            df = read_hdfs_csv(file_path)
            newdf = df.append(self.runlog_configs_df)
            write_hdfs_csv(file_path, newdf)

            file_name = csv_filenames["logs"]
            file_path = f"{self.main_path}/{file_name}"
            df = read_hdfs_csv(file_path)
            newdf = df.append(self.runlog_logs_df)
            write_hdfs_csv(file_path, newdf)

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
