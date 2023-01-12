# """Define helper functions that wrap regularly-used functions."""

from functools import wraps
# import logging

import os
from random import randint
from time import time

# import pandas as pd
# from datetime import datetime

# LOGGER = logging.getLogger(__name__)


def generate_run_id():
    """Generate a run ID using the date and a sequentially increasing number.

   
    """

    date = datetime.now().strftime("%Y-%m-%d")
    
    try:
        spark.sql("REFRESH {database}.vat_runlog")
        ids = get_latest_runid(spark, database, "global")
        ids = int(ids[15:]) if ids[4:14] == date else 0
    except:
        ids = 0

    run_id = f"VAT_{date}_{ids + 1}"

    return run_id


# def write_versioned_csv(spark, df, path, file_prefix, run_id=None, partitions=1):
#     """Write a dataframe to a versioned csv file in HDFS.

#     Determine the version number by checking the folder that file is
#     being written to.

#     Parameters
#     ----------
#     df : DataFrame
#         The dataframe to be saved as a csv file.
#     path : str
#         The path to the folder to save the file.
#     file_prefix : str
#         The filename prefix of the file being written.
#     run_id: str
#         The run_id of the current pipeline run
#     partitions: int
#         The number of partitions to divide the csv into
#     """
#     existing_files = hdfs.read_dir_files(path)
#     valid_files = [i for i in existing_files if i.startswith(file_prefix)]
    
#     if valid_files:
#         ver_file = max(valid_files)
#         pos = len(file_prefix) + 2
#         ver = "_v" + str(int(ver_file[pos:pos + 3]) + 1).zfill(3)
#     else:
#         ver = "_v001"

#     file_path = os.path.join(path, file_prefix + ver)
#     hdfs.write_csv_rename(df, file_path, file_partitions=partitions)


# def csv_to_df(spark, filepath, expected_columns):
#     """Load a CSV into a dataframe and check it has the correct columns.

#     Parameters
#     ----------
#     spark : SparkContext
#         The spark context being used.
#     filepath : str
#         The full path and filename of the CSV file to load.
#     expected_columns : list
#         A list containing the expected columns.

#     Returns
#     -------
#     DataFrame
#         A spark dataframe containing the data from the CSV file.
#     """
#     try:
#         spark_df = spark.read.csv(filepath, header=True)
#         LOGGER.info(f"Loaded CSV file {filepath} from HDFS")
#     except AnalysisException:
#         if os.path.exists(filepath) is True:
#             pandas_df = pd.read_csv(filepath, dtype=str, na_filter=False)
#             spark_df = spark.createDataFrame(pandas_df)
#             LOGGER.info(f"Loaded CSV file {filepath} from local filesystem")
#         else:
#             LOGGER.error(f"Couldn't find file {filepath}")
#             raise FileNotFoundError

#     if not (all(elem in spark_df.columns for elem in expected_columns)):
#         LOGGER.error(f"Could not find specified columns in {filepath}")
#         LOGGER.info("Columns expected: " + str(expected_columns))
#         LOGGER.info("Columns in file: " + str(spark_df.columns))
#         raise ValueError

#     return spark_df
  
    
# def write_to_hive(spark, df, db, hive_table_name):
#     """Save a dataframe to a Hive table, overwriting any existing table.

#     Parameters
#     ----------
#     df : DataFrame
#         The dataframe to save as a Hive table.
#     db : str
#         The name of the Hive database to save the data to.
#     hive_table_name: str
#         The name of the Hive table to overwrite.

#     """
#     _save_to_hive(spark, df, db, hive_table_name, "overwrite")
#     LOGGER.info(f"Wrote Hive table {hive_table_name}")


# def append_to_hive(spark, df, db, hive_table_name):
#     """Append a dataframe to a Hive table.

#     Parameters
#     ----------
#     df : DataFrame
#         The dataframe to append to the Hive table.
#     db : str
#         The name of the Hive database to save the data to.
#     hive_table_name: str
#         The name of the Hive table to append to.

#     """
#     _save_to_hive(spark, df, db, hive_table_name, "append")
#     LOGGER.info(f"Appended data to Hive table {hive_table_name}")


# def _save_to_hive(spark, df, db, table_name, mode):
#     """Write a dataframe as a Hive table.

#     Parameters
#     ----------
#     df : DataFrame
#         The dataframe to be saved as a Hive table.
#     db : str
#         The name of the Hive database to save the data to.
#     table_name : str
#         The name of the Hive table to save the data to.
#     mode : str
#         The write mode, either append or overwrite.

#     """
#     hive_table = db + "." + table_name

#     if isinstance(df, DataFrame):
#         df.write.mode(mode).format("parquet").saveAsTable(hive_table)

# def get_hive_init(val):
#     """Return part of hive table name that is determined by 
#     Turnover or Expenditure.
    
#      Parameters
#     ----------
#     val: String
#         String indicating whether to use turnover or expenditure.
    
#     """ 
#     if val.lower() == "turnover":
#         return "to"
#     elif val.lower() == "expenditure":
#         return "exp"
#     else:
#         raise Exception(f"""Do not recognise {val}. Can only accept turnover 
#                       or expenditure.""")


# def timeit(msg, _print=print):
#     """Outer decorator to use additional parameters"""
#     def logger(func):
#         """Logger decorator"""
        
#         # retain special values of the function i.e. f._doc and f._name_,
#         # otherwise this is overwritten  by the logger function's values
#         @wraps(func)
#         def inner_logger(*args, **kwargs):
#             """Log the execution time of a function"""
#             start = time()
            
#             output = func(*args, **kwargs)
            
#             end = time() - start
            
#             # format execution time in minutes or seconds
#             timing = f'{round(end/60, 2)}m' if end > 60 else f'{round(end, 1)}s\n'
            
#             _print(f" {msg}: {timing}")
            
#             return output
        
#         return inner_logger
  
      
# def get_latest_runid(spark, database, module):
#     """Return latest run ID from runlog.
   
#     Parameters
#     ----------
#     module: String
#         Which module to enquire about.
  
#     """
#     latest = f"SELECT Max(datetime) FROM {database}.vat_runlog where module = '{module}'"
#     latest = spark.sql(latest).first()["max(datetime)"]
#     query = f"SELECT run_id FROM {database}.vat_runlog WHERE datetime = '{latest}'"

#     return spark.sql(query).first()["run_id"]

# def get_latest_timestamp(spark, module):
#     """Return modules latest timestamp for load time.
   
#     Parameters
#     ----------
#     module: String
#         Which module to enquire about.
  
#     """
#     latest = f"SELECT Max(load_timestamp) FROM {module}"
#     latest = spark.sql(latest)
#     latest = latest.first()["max(load_timestamp)"]
    
#     return latest

# def get_latest_filname(spark, module):
#     """Returns the latest filename.
    
#     Parameters
#     ----------
#     module: String
#         Which module to enquire about.
#     """
#     timestamp = get_latest_timestamp(spark, module)
#     latest = f"SELECT DISTINCT input_file_name FROM {module} "\
#              f"WHERE load_timestamp = '{timestamp}'"
#     latest = spark.sql(latest)
#     latest = latest.first()["input_file_name"]

#     return latest

# def log_module(module):
#     """Logger message for the start of a module.
#     Parameters
#     ----------
#     module: String
#         Which module to message about.
#     """
#     LOGGER.info("")
#     LOGGER.info(f"Starting {module}")
#     LOGGER.info("==============================================")