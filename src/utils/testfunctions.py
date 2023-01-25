from src.utils.wrappers import time_logger_wrap, exception_wrap
import time

# logging.config.fileConfig("src/utils/testconfig.ini", disable_existing_loggers=False)
# logger = logging.getLogger(__name__)


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
        time.sleep(5.5)
    return ans


@time_logger_wrap
@exception_wrap
def add(a, b):
    """Testing multiple wrappers for one function"""
    c = a + b
    time.sleep(0.5)
    return c
