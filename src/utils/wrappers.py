import logging
from functools import wraps
from time import perf_counter
import traceback

# from table_logger import TableLogger
import logging.config


logger = logging.getLogger(__name__)


def logger_creator(config):
    """Set up config for logging. This method overwrites
    the previously saved logs after moving them to csv format.
    This function returns a custom logger that is called
    in the main script before running the pipeline"""
    logging.basicConfig(
        # logging level is obtained from user configs
        level=config["logging_level"],
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
