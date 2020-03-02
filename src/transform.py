import os
import sys
import logging
import pandas as pd
import numpy as np
import json
from scipy.stats import expon
from typing import Dict, NewType, Any
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

def get_return_date(df: DataFrame, conf: Dict[Any, Any]) -> Series:
    # logging.debug(f'Transforming {df.shape[0]} records in {sys._getframe(  ).f_code.co_name}...')
    ship_date = df['ship_date']
    order_date = df['order_date']
    rd = []
    for sd, od in zip(ship_date, order_date):
        sd = pd.to_datetime(sd, format="%Y-%m-%d")
        od = pd.to_datetime(od, format="%Y-%m-%d")
        return_date = calculate_return_date(od, conf)
        while return_date < sd:
            return_date = calculate_return_date(od, conf)
        rd.append(return_date)
    return pd.Series(rd)

def calculate_return_date(timestamp, conf):
    return timestamp + \
        pd.Timedelta(np.random.randint(low=conf['return_date_low'],
                                       high=conf['return_date_high'],
                                       size=1)[0], unit='D')

def get_expon(n: int, s: int = 10) -> int:
    v: int = int(round(expon.rvs(loc=0, scale=n/s)))
    return v if v <= n else v

def get_return_quantity(df: DataFrame) -> Series:
    logging.debug(f'Transforming {df.shape[0]} records in {sys._getframe(  ).f_code.co_name}...')
    res: List[float] = []
    for i, r in df.iterrows():
        # modify the rate of return by s parameter
        # higher s parameter is lower rate of return
        # 24.65 is about a 5% rate of return, 50k out of 1M
        res.append(get_expon(r['quantity'], s=24.655))
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

def get_shifted_order_id(df: DataFrame, conf: Dict[Any, Any]) -> Series:
    logging.debug(f'Transforming {df.shape[0]} records in {sys._getframe(  ).f_code.co_name}...')
    y1 = df['order_id'].apply(lambda x: x.replace('-2011-', '-' + conf['y1_mapping'] + '-'))
    y2 = y1.apply(lambda x: x.replace('-2012-', '-' + conf['y2_mapping'] + '-'))
    y3 = y2.apply(lambda x: x.replace('-2013-', '-' + conf['y3_mapping'] + '-'))
    y4 = y3.apply(lambda x: x.replace('-2014-', '-' + conf['y4_mapping'] + '-'))
    y5 = y4.apply(lambda x: x.replace('-2015-', '-' + conf['y5_mapping'] + '-'))
    return y5

def get_shifted_order_date(df: DataFrame, conf: Dict[Any, Any]) -> Series:
    logging.debug(f'Transforming {df.shape[0]} records in {sys._getframe(  ).f_code.co_name}...')

    leap_year_clean = df['order_date'].apply(lambda x: x.replace('-02-29', '-02-28'))
    y1 = leap_year_clean.apply(lambda x: x.replace('2011', conf['y1_mapping']))
    y2 = y1.apply(lambda x: x.replace('2012', conf['y2_mapping']))
    y3 = y2.apply(lambda x: x.replace('2013', conf['y3_mapping']))
    y4 = y3.apply(lambda x: x.replace('2014', conf['y4_mapping']))
    y5 = y4.apply(lambda x: x.replace('2015', conf['y5_mapping']))
    return pd.to_datetime(y5, format="%Y-%m-%d")

def get_shifted_ship_date(df: DataFrame, conf: Dict[Any, Any]) -> Series:
    logging.debug(f'Transforming {df.shape[0]} records in {sys._getframe(  ).f_code.co_name}...')
    leap_year_clean = df['ship_date'].apply(lambda x: x.replace('-02-29', '-02-28'))
    y1 = leap_year_clean.apply(lambda x: x.replace('2011', conf['y1_mapping']))
    y2 = y1.apply(lambda x: x.replace('2012', conf['y2_mapping']))
    y3 = y2.apply(lambda x: x.replace('2013', conf['y3_mapping']))
    y4 = y3.apply(lambda x: x.replace('2014', conf['y4_mapping']))
    y5 = y4.apply(lambda x: x.replace('2015', conf['y5_mapping']))
    return pd.to_datetime(y5, format="%Y-%m-%d")

def get_shifted_return_date(df: DataFrame, conf: Dict[Any, Any]) -> Series:
    logging.debug(f'Transforming {df.shape[0]} records in {sys._getframe(  ).f_code.co_name}...')
    y1 = df['return_date'].apply(lambda x: x.replace('2011', conf['y1_mapping']))
    y2 = y1.apply(lambda x: x.replace('2012', conf['y2_mapping']))
    y3 = y2.apply(lambda x: x.replace('2013', conf['y3_mapping']))
    y4 = y3.apply(lambda x: x.replace('2014', conf['y4_mapping']))
    y5 = y4.apply(lambda x: x.replace('2015', conf['y5_mapping']))
    return y5

def get_shifted_date_rank(df: DataFrame, conf: Dict[Any, Any]) -> Series:
    logging.debug(f'Transforming {df.shape[0]} records in {sys._getframe(  ).f_code.co_name}...')
    y1 = df['date_rank'].apply(lambda x: x.replace('2011', conf['y1_mapping']))
    y2 = y1.apply(lambda x: x.replace('2012', conf['y2_mapping']))
    y3 = y2.apply(lambda x: x.replace('2013', conf['y3_mapping']))
    y4 = y3.apply(lambda x: x.replace('2014', conf['y4_mapping']))
    y5 = y4.apply(lambda x: x.replace('2015', conf['y5_mapping']))
    return y5

###########
# Customers
###########

def get_postal_code(df: DataFrame) -> Series:
    logging.debug(f'Transforming {df.shape[0]} records in {sys._getframe(  ).f_code.co_name}...')
    return df['postal_code'].astype('object')
