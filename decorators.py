import functools
import time
import logging


def allow_logging(func):
    """Wrapper for logging - announces function start (with name, arguments) and function finish including its duration.
    """
    functools.wraps(func)

    def wrapper(*args, **kwargs):
        logger = logging.getLogger()
        logger.setLevel("DEBUG")
        t0 = time.perf_counter()
        logging.info(
            f"### Starting function: {func.__name__} called with arguments: {args}"
        )
        value = func(*args, **kwargs)
        t1 = time.perf_counter()
        logging.info(f"### Finished in {t1 - t0:.2f} seconds")
        return value

    return wrapper
