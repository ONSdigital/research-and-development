import logging
from functools import wraps
from time import perf_counter
import traceback
from datetime import datetime
from table_logger import TableLogger
import configparser

"""Wrappers to handle logging of functions."""

table_config = "SingleLine"


config = configparser.ConfigParser()
config.read(r"src/utils/testconfig.ini")
global_config = config["global"]

# Create the logger which can be imported into any module for logging
logger = logging.getLogger(__name__)


def get_logging_level_command(lev_from_conf):
    """Translate a single word into a logging level command."""
    loglevels_dict = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
    }
    logging_level = loglevels_dict[lev_from_conf]
    return logging_level


def logger_creator(global_config):
    """Set up config for logging."""
    logging_level = get_logging_level_command(global_config["logging_level"])
    log_to_file = eval(global_config["log_to_file"])
    # How to log is determined by log_to_file in the config
    if log_to_file:
        # Add a unique timestamp string to avoid overwriting
        timestamp_string = datetime.now().strftime("%Y-%m-%d %H%M%S")
        # Create log handlers so logs are written to file and stdout
        log_handlers = [
            logging.FileHandler(f"src/utils/savedlogs_{timestamp_string}.log"),
            logging.StreamHandler(),
        ]

        logging.basicConfig(
            level=logging_level,
            format="%(asctime)-15s %(levelname)-8s %(message)s",
            handlers=log_handlers,
        )
    else:
        logging.basicConfig(
            level=logging_level, format="%(asctime)-15s %(levelname)-8s %(message)s"
        )
    return logger


def time_logger_wrap(func):
    """Define a decorator to log the time at the start and end of a function."""

    @start_finish_wrapper
    @wraps(func)
    def decorator(*args, **kwargs):
        """Define the decorator itself."""
        enter_time = starting_time()
        result = func(*args, **kwargs)
        finishing_time(func, enter_time)
        return result

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


# logger_creator(global_config)
if __name__ == "__main__":

    @exception_wrap
    def divbyzero(num):
        """Define a testing function that will throw an exception."""
        ans = num / 0
        return ans

    @exception_wrap
    def this_definitely_works(num):
        """Define a testing function that will not throw an exception."""
        ans = num**num
        return ans

    @time_logger_wrap
    def takes_a_while(num):
        """Define a testing function that takes some time to run."""
        ans = 0
        for _ in range(num):
            ans += (num**2) ** 2
        return ans

    # Calling functions to test the logging wrappers
    this_definitely_works(10)
    print(takes_a_while(10000))
    divbyzero(10)
