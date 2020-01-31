import re
import os
import sys
import logging
import pandas as pd
import numpy as np
from util.helper import timer
from typing import Dict, NewType, Any
from config.config import DIR_DATA, DIR_CONFIG, DIR_OUTPUT

DataFrame = NewType('DataFrame', pd.DataFrame)
Series = NewType('Series', pd.Series)


def interpolate(df: DataFrame, conf: Dict[Any, Any]) -> DataFrame:
    '''
    Interpolates data to user specified number of rows
    as dictated by input config dictionary
    '''
    logging.debug(f'Extrapolating data in {sys._getframe(  ).f_code.co_name}...')

    # set an index for proportional sampling
    df['date_rank'] = pd.to_datetime(df['order_date'])
    df.sort_values('date_rank', inplace=True)
    df['index_rank'] = df.reset_index().index

    # create weights array for ship mode sampling
    ship_mode_unique_vals = df['ship_mode'].unique().tolist()
    ship_mode_distribution = df['ship_mode'].value_counts() / df['ship_mode'].value_counts().sum()
    ship_mode_distribution_list = ship_mode_distribution.tolist()

    # generate a set of order ids to enforce generated PK uniqueness
    uids = set()
    def compile_uids(x):
        n = int(x[x.rfind('-')+1:])
        uids.add(n)
        return n
    order_uids = df['order_id'].apply(compile_uids)
    max_uid = order_uids.max()
    min_uid = order_uids.min()
    del order_uids

    # begin creating records
    out_df = df.copy()

    for i in range(conf['desired_output_rows'] - df.shape[0]):
        n = i + df.shape[0]
        if n%1000 == 0:
            logging.debug(f'Bootstrapping record number {n}')
        samp = df.sample(1, weights=df['index_rank'])
        out_df = out_df.append(generate_row(
            samp,
            ship_mode_unique_vals,
            ship_mode_distribution_list,
            conf,
            max_uid,
            uids
        ), ignore_index=True)

    logging.debug(f'Returned {out_df.shape[0]} records in {sys._getframe(  ).f_code.co_name}...')
    out_df.to_csv(os.path.join(DIR_OUTPUT, 'raw_bootstrapped.csv'))
    return out_df

##################
# Helper Functions
##################

def generate_row(df, ship_mode_unique_vals, ship_mode_distribution_list, conf, max_uid, uids):
    # single row df
    samp = df

    od = generate_order_or_ship_date(samp['order_date'].values[0], conf['order_date_low'], conf['order_date_high'])
    sd = generate_order_or_ship_date(od, conf['ship_date_low'], conf['ship_date_high'])
    # oid = generate_order_id(samp['order_id'].values[0], od.year, max_uid, uids)['generated_order_id']
    oid = generate_order_id(samp['order_id'].values[0], od.year, max_uid, uids)
    sm = generate_ship_mode(ship_mode_unique_vals, ship_mode_distribution_list)
    dis, prof = generate_discount_and_profit(samp['discount'].values[0], od.year, conf).values()

    res = samp.copy()
    res['order_id'] = oid
    res['ship_mode'] = sm
    res['discount'] = dis
    res['profit'] = prof

    return res

def get_random_id(min_id, max_id, dtype='int'):
    return np.random.randint(min_id, max_id, dtype=dtype)

def get_uid(min_id, max_id, uids):
    '''
    Fetches UID that does not conflict with existing
    dataset UIDs
    '''
    rid = get_random_id(min_id, max_id)
    while rid in uids:
        rid =  get_random_id(min_id, max_id)
    return rid

def generate_order_or_ship_date(x, date_low, date_high):
    if isinstance(x, pd.Timestamp):
        x = x.to_datetime64()
    return x + pd.Timedelta(np.random.randint(low=date_low, high=date_high, size=1)[0], unit='D')

def generate_order_id(x, target_year, max_uid, uids):
    prefix, _, uid = x.split('-')
    return '-'.join(
        [prefix, str(target_year), str(get_uid(uid, max_uid, uids))])

def generate_ship_mode(vals, dist):
    return np.random.choice(vals, p=dist)

def generate_discount_and_profit(x, year, discount_dict):
    amt = discount_dict['discounts'][str(year)]
    discount = x + amt
    profit = x - (x*amt)
    return {
        'discount': discount,
        'profit': profit
    }
