import os
import sys
import logging
import pandas as pd
import numpy as np
import json
from scipy.stats import expon
from typing import Dict, NewType
from config.config import DIR_DATA, DIR_CONFIG

DataFrame = NewType('DataFrame', pd.DataFrame)
Series = NewType('Series', pd.Series)

##########
# Products
##########

def get_product_cost_to_consumer(df: DataFrame) -> Series:
    logging.debug(f'Transforming {df.shape[0]} records in {sys._getframe(  ).f_code.co_name}...')
    res: List[float] = []
    for i, r in df.iterrows():
        res.append(round(r['sales'] / r['quantity'], 2))
    return pd.Series(data=res, name='product_cost_to_consumer')

#########
# Regions
#########

def get_country_code(df: DataFrame) -> Series:
    logging.debug(f'Transforming {df.shape[0]} records in {sys._getframe(  ).f_code.co_name}...')
    # thank you country.io, mods needed
    # for myanmar and cote d'ivoire
    with open(os.path.join(DIR_CONFIG, './country_codes.json')) as f:
        cc_lookup: Dict[str, str] = json.loads(f.read())
    cc_rev_lookup: Dict[str, str] = {}
    for k, v in cc_lookup.items():
        cc_rev_lookup[v] = k
    return df.copy()['country'].apply(lambda x: cc_rev_lookup.get(x) or 'XX')

def get_salesperson(df: DataFrame) -> Series:
    logging.debug(f'Transforming {df.shape[0]} records in {sys._getframe(  ).f_code.co_name}...')
    with open(os.path.join(DIR_CONFIG, 'regions.json')) as f:
        sp_lookup: Dict[str, str] = json.loads(f.read())
    return df.copy()['region'].apply(lambda x: sp_lookup.get(x) or 'Unknown')

#########
# Returns
#########

def get_return_date(df: DataFrame) -> Series:
    logging.debug(f'Transforming {df.shape[0]} records in {sys._getframe(  ).f_code.co_name}...')
    # using low=6 for assumption of 6 days from ship to receive shipment
    # using 90 day return window as a reasonable company policy for returns
    return pd.to_datetime(df['ship_date']) + \
        pd.Timedelta(np.random.randint(low=6, high=90, size=1)[0], unit='D')

def get_expon(n: int, s: int = 10) -> int:
    v: int = int(round(expon.rvs(loc=0, scale=n/s)))
    return v if v <= n else v

def get_return_quantity(df: DataFrame) -> Series:
    logging.debug(f'Transforming {df.shape[0]} records in {sys._getframe(  ).f_code.co_name}...')
    res: List[float] = []
    for i, r in df.iterrows():
        res.append(get_expon(r['quantity']))
    return pd.Series(data=res, name='return_quantity')

def get_reason_returned(df: DataFrame) -> Series:
    logging.debug(f'Transforming {df.shape[0]} records in {sys._getframe(  ).f_code.co_name}...')
    res: List[str] = []
    for i, r in df.iterrows():
        res.append(
            np.random.choice(
                ['Wrong Color',
                 'Not Given',
                 'Wrong Item',
                 'Not Needed'],
                replace=True,
                p = [0.15, 0.4, 0.3, 0.15]
            )
        )
    return pd.Series(data=res, name='reason_returned')

########
# Orders
########

def get_discount(df: DataFrame) -> Series:
    logging.debug(f'Transforming {df.shape[0]} records in {sys._getframe(  ).f_code.co_name}...')
    with open(os.path.join(DIR_CONFIG, './discounts.json')) as f:
        discount_lookup: Dict[str, float] = json.loads(f.read())
    res: List[float] = []
    for i, r in df.iterrows():
        res.append(
            discount_lookup['sub_category'][r['sub_category']] +\
            discount_lookup['region'][r['region']]
        )
    return pd.Series(data=res, name='discount')

###########
# Customers
###########

def get_postal_code(df: DataFrame) -> Series:
    logging.debug(f'Transforming {df.shape[0]} records in {sys._getframe(  ).f_code.co_name}...')
    return df['postal_code'].astype('object')
