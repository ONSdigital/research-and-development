import logging
from functools import wraps
from time import perf_counter
import traceback
import pandas as pd

import logging.config


logger = logging.getLogger(__name__)


def logger_creator(global_config):
    """Set up config for logging. This method overwrites
    the previously saved logs after moving them to csv format.
    This function returns a custom logger that is called
    in the main script before running the pipeline"""
    logging.basicConfig(
        # logging level is obtained from user configs
        level=global_config["logging_level"],
        # Define the detail and order of the written logs
        format="%(asctime)s - %(name)s - %(funcName)s - %(levelname)s:%(message)s",
        # Log to both console and file
        handlers=[
            logging.FileHandler("logs/main.log", mode="w"),
            logging.StreamHandler(),
        ],
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
            # run the function as is.
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


def df_change_func_wrap(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Extract the dataframe arg
        df_arg = None
        for arg in args:
            if isinstance(arg, pd.DataFrame):
                df_arg = arg
                break

        if df_arg is None:
            raise ValueError("No dataframe found in arguments")

        # Get shape
        before_shape = df_arg.shape

        # Apply the function
        result = func(*args, **kwargs)

        # Get shape after function
        after_shape = result.shape

        # Extract rows and col
        rows_before = before_shape[0]
        rows_after = after_shape[0]

        cols_before = before_shape[1]
        cols_after = after_shape[1]

        rows_diff = rows_after - rows_before
        cols_diff = cols_after - cols_before

        def _change_direction(before, after):
            """Get the direction of the change."""
            # Evalutate the direction of change
            change = ["gained", "removed"][after < before]

            return change

        # Get direction of change
        row_change = _change_direction(rows_before, rows_after)
        col_change = _change_direction(cols_before, cols_after)

        if rows_diff == 0 and cols_diff == 0:
            logger.info("There has been no change in the dataframe")
        else:
            logger.info("Changes to the dataframe are as follows")
            logger.info(f"{abs(rows_diff)} rows were {row_change}")
            logger.info(f"{abs(cols_diff)} columns were {col_change}")

        return result

    return wrapper


def count_split_records_wrap(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Extract the dataframe arg
        df_arg = None
        for arg in args:
            if isinstance(arg, pd.DataFrame):
                df_arg = arg
                break

        if df_arg is None:
            raise ValueError("No dataframe found in arguments")

        # Get shape
        before_shape = df_arg.shape

        # Apply the function
        filtered_df, excluded_df = func(*args, **kwargs)

        # Get shape after function
        after_shape = filtered_df.shape

        # Extract rows and col
        rows_before = before_shape[0]
        rows_after = filtered_df.shape[0] + excluded_df.shape[0]

        cols_before = before_shape[1]
        cols_after = after_shape[1]

        rows_diff = rows_after - rows_before
        cols_diff = cols_after - cols_before

        def _change_direction(before, after):
            """Get the direction of the change."""
            # Evalutate the direction of change
            change = ["gained", "removed"][after < before]

            return change

        # Get direction of change
        row_change = _change_direction(rows_before, rows_after)
        col_change = _change_direction(cols_before, cols_after)

        if rows_diff == 0 and cols_diff == 0:
            logger.info("There has been no change in the dataframe")
        else:
            logger.info("Changes to the dataframe are as follows")
            logger.info(f"{abs(rows_diff)} rows were {row_change}")
            logger.info(f"{abs(cols_diff)} columns were {col_change}")

        return filtered_df, excluded_df

    return wrapper


def validate_dataframe_not_empty(func):
    def wrapper(df, *args, **kwargs):
        if df.empty:
            logger.warning("Input dataframe is empty.")
            raise ValueError("Input dataframe is empty.")
        return func(df, *args, **kwargs)

    return wrapper
