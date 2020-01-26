import sys
import logging
import pandas as pd
from typing import Dict, NewType

DataFrame = NewType('DataFrame', pd.DataFrame)
Series = NewType('Series', pd.Series)


def get_postal_code(df: DataFrame) -> Series:
    logging.debug(f'Retrieving postal code in {sys._getframe(  ).f_code.co_name}...')
    return df['postal_code'].astype('object')
