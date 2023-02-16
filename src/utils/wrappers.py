import logging
from functools import wraps
from time import perf_counter
import traceback
import datetime
# from table_logger import TableLogger
import logging.config
import configparser

table_config = "SingleLine"

logger = logging.getLogger(__name__)

def logger_creator(global_config, run_id):
    """Set up config for logging."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H%M%S")
    if global_config["log_to_file"] == True:
        logging.basicConfig(level="DEBUG",
        format="%(asctime)s - %(name)s - %(funcName)s - %(levelname)s:%(message)s",
        handlers=[logging.FileHandler(f"logs/{timestamp}_{run_id}.log"),
        logging.StreamHandler()]
        )
        logger = logging.getLogger(__name__)
    else:
        logging.basicConfig(
            level="DEBUG", format="%(asctime)s - %(levelname)s:%(message)s"
        )
        logger = logging.getLogger(__name__)
    return logger



def time_logger_wrap(func):
    """Define a decorator to log the time at the start and end of a function."""

    @start_finish_wrapper
    @wraps(func)
    def decorator(*args, **kwargs):
        """Define the decorator itself."""
        enter_time = starting_time()
        result = func(*args, **kwargs)
        time_taken = finishing_time(func, enter_time)
        return result, time_taken

    return decorator


def starting_time():
    """Record the start time of a function."""
    enter_time = perf_counter()
    return enter_time


def finishing_time(func, enter_time):
    """Record the end time of a function then calculate and log its runtime."""
    exit_time = perf_counter()
    logger.debug(
        f"Running the {func.__name__} function took: "
        f"{round((exit_time-enter_time), 2)} seconds"
    )
    


def start_finish_wrapper(func):
    """Define a decorator to log the beginning and end of a function."""

    @wraps(func)
    def decorator(*args, **kwargs):
        """Define the decorator itself."""
        log_start(func)
        result = func(*args, **kwargs)
        log_finish(func)
        return result

    return decorator


def log_start(func):
    """Log the start of a function at INFO level."""
    logger.info(f"Starting the {func.__name__} function")


def log_finish(func):
    """Log the end of a function at INFO level."""
    logger.info(f"Finished the {func.__name__} function")


def method_start_finish_wrapper(func):
    """Define a decorator to log the start time and end time of a function."""

    @wraps(func)
    def decorator(*args, **kwargs):
        """Define the decorator itself."""
        meth_log_start(func)
        result = func(*args, **kwargs)
        meth_log_finish(func)
        return result

    return decorator


def meth_log_start(func):
    """Log the start of a function at DEBUG level."""
    logger.debug(f"Starting the {func.__name__} function")


def meth_log_finish(func):
    """Log the end of a function at DEBUG level."""
    logger.debug(f"Finished the {func.__name__} function")


def exception_wrap(func):
    """Define a decorator to log all exceptions that occur in a function."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        """Define the decorator itself."""
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            # log the exception
            err = f"There was an exception in function {func.__name__}"
            logger.info(err)
            logger.error(f"Exception: {e}", exc_info=True)
            traceback.print_exc()
            raise

    return wrapper


def df_change_wrap(func):
    """Define a decorator to log the difference between the input and output.

    This wrapper calculates the differences between the input dataframe
    and the output dataframe,
    helping to keep a record of changes to the dataframe.
    This wrapper should decorate the main
    method of a class, e.g. for the `DuplicateWaves` class,
    this decorate is applied to the
    `duplicate_data()` method. The method this is applied to
    must have must have `self` as an
    argument and must operate on and then return self.vf_df.
    The class as a whole also requires
    the table_config attribute to be set.
    """

    @wraps(func)
    def call(*args, **kwargs):
        """Define the decorator itself."""
        self_ = args[0]
        pre_df = self_.vf_df
        pre_rows, pre_cols = pre_df.shape[0], pre_df.shape[1]
        table_config = self_.table_config
        post_df = func(*args, **kwargs)
        logger.info(f"{func.__name__} changed the df shape as follows:")
        df_measure_change(post_df, pre_rows, pre_cols, table_config)
        return post_df

    return call


def df_measure_change(df, rows_before, cols_before, table_config):
    """Log the change in a dataframe caused by a function."""
    shape = df.shape
    rows_after, cols_after = shape[0], shape[1]

    def _change_direction(before, after):
        """Get the direction of the change."""
        # Evalutate the direction of change
        change = ["gained", "removed"][after < before]
        return change

    row_change = _change_direction(rows_before, rows_after)
    col_change = _change_direction(cols_before, cols_after)

    # Get absolute difference in change
    row_diff = abs(rows_after - rows_before)
    col_diff = abs(cols_after - cols_before)

    if table_config == "Table":
        # Make table to log the changes in df
        tbl = TableLogger(
            columns="df Changes,Rows,Columns",
            float_format="{:,.2f}".format,
            default_colwidth=15,
        )

        tbl("From", rows_before, cols_before)
        tbl("To", rows_after, cols_after)
        tbl("Change", f"{row_diff} ({row_change})", f"{col_diff} ({col_change})")
    elif table_config == "SingleLine":
        logger.info(
            f"""Difference in rows: {row_diff} ({row_change}),
                        Difference in columns {col_diff} ({col_change})"""
        )
    else:
        logger.warning(
            """Trouble at mill!!! Mistake in config.
                          Either 'Table' or 'SingleLine' must be specified."""
        )
