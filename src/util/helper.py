import sys
import logging
import functools
import time
import pandas as pd
from typing import Dict, NewType

DataFrame = NewType('DataFrame', pd.DataFrame)
Series = NewType('Series', pd.Series)


def get_postal_code(df: DataFrame) -> Series:
    logging.debug(f'Retrieving postal code in {sys._getframe(  ).f_code.co_name}...')
    return df['postal_code'].astype('object')

def timer(func):
    """Print the runtime of the decorated function"""
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        logging.debug(f'Finished {func.__name__!r} in {run_time:.4f} secs')
        return value
    return wrapper_timer
