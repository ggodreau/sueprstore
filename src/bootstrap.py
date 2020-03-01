import re
import os
import shutil
import sys
import logging
import pandas as pd
import numpy as np
from pprint import pprint
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
    logging.debug(f'Interpolating data in {sys._getframe(  ).f_code.co_name}...')

    # set an index for proportional sampling
    df['date_rank'] = pd.to_datetime(df['order_date'])
    df.sort_values('date_rank', inplace=True)
    df['index_rank'] = df.reset_index().index

    # generate a set of order ids to enforce generated PK uniqueness
    uids = set()
    def compile_uids(x):
        n = int(x[x.rfind('-')+1:])
        uids.add(n)
        return n
    order_uids = df['order_id'].apply(compile_uids)
    max_uid = order_uids.max() + 1
    min_uid = order_uids.min()
    diff_uid = max_uid - min_uid
    logging.debug(f'UID max: {max_uid} min: {min_uid} diff: '
                  f'{diff_uid - df.shape[0]} {sys._getframe(  ).f_code.co_name}...')
    if diff_uid <= (conf['desired_output_rows'] - df.shape[0]):
        raise Exception(f'Unable to produce {conf["desired_output_rows"]}'
                        f' with available number of UIDs')
    del order_uids

    # clean up any previously existing output files
    for file_name in os.listdir(DIR_OUTPUT):
        file_path = os.path.join(DIR_OUTPUT, file_name)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            logging.error(f'Failed to delete {file_path}. Reason: {e}')

    # generate rows in shard_size increments
    out_dict = {}
    for i in range(conf['desired_output_rows'] - df.shape[0]):
        n = i + df.shape[0]
        samp = df.sample(1, weights=df['index_rank'])
        out_dict[n] = generate_row(
            samp,
            conf,
            min_uid,
            max_uid,
            uids).iloc[0,:].to_dict()

        if n%conf['shard_size'] == 0:
            logging.debug(f'Bootstrapping record number {n}')
            pd.DataFrame.from_dict(out_dict, orient='index').to_csv(
                os.path.join(DIR_OUTPUT, f'bootshard_{n - conf["shard_size"]}.csv'),
                index_label='id',
                header=False)
            out_dict = {}

    # write remaining rows to file
    pd.DataFrame.from_dict(out_dict, orient='index').to_csv(
        os.path.join(DIR_OUTPUT, f'bootshard_{n - conf["shard_size"]}.csv'),
        index_label='id',
        header=False)
    del out_dict

    # recombine shards
    shards = []
    for f in os.listdir(DIR_OUTPUT):
        if re.search(r'^bootshard_\d+.csv$', f):
            shards.append(f)

    df.to_csv(
        os.path.join(DIR_OUTPUT, 'bootshard_compiled.csv'),
        index_label='id',
        header=True,
        mode='w')

    for s in shards:
        df_s = pd.read_csv(
            os.path.join(DIR_OUTPUT, s),
            index_col=0,
            header=None
        )
        df_s.to_csv(
            os.path.join(DIR_OUTPUT, 'bootshard_compiled.csv'),
            index_label='id',
            header=False,
            mode='a')
        logging.debug(f'Completed compiling shard num: {s}')

    logging.debug(f'Processed {df.shape[0] + conf["desired_output_rows"]} records in {sys._getframe(  ).f_code.co_name}...')
    compiled_df = pd.read_csv(os.path.join(DIR_OUTPUT, 'bootshard_compiled.csv'), index_col='id')
    return compiled_df


##################
# Helper Functions
##################

def generate_row(df, conf, min_uid, max_uid, uids):
    # single row df
    samp = df

    od = generate_order_or_ship_date(samp['order_date'].values[0], conf['order_date_low'], conf['order_date_high'])
    oid = generate_order_id(samp['order_id'].values[0], od.year, min_uid, max_uid, uids)
    ship_low, ship_high, ship_mode = conf['ship_delay'][samp['category'].values[0]][samp['sub_category'].values[0]]
    sd = generate_order_or_ship_date(od, ship_low, ship_high)
    dis, prof = generate_discount_and_profit(samp['discount'].values[0], od.year, conf).values()

    res = samp.copy()
    res['order_date'] = od
    res['order_id'] = oid
    res['ship_mode'] = ship_mode
    res['ship_date'] = sd
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
    i = 0
    while rid in uids:
        if i > 100000:
            if i%100000 == 0:
                logging.warn(f'{sys._getframe(  ).f_code.co_name} is hanging with i: {i}, min_id: {min_id}, max_id: {max_id}, rid: {rid}...')
        rid = get_random_id(min_id, max_id)
        i += 1
    return rid

def generate_order_or_ship_date(x, date_low, date_high):
    if isinstance(x, pd.Timestamp):
        x = x.to_datetime64()
    return x + pd.Timedelta(np.random.randint(low=date_low, high=date_high+1, size=1)[0], unit='D')

def generate_order_id(x, target_year, min_uid, max_uid, uids):
    prefix, _, uid = x.split('-')
    return '-'.join(
        [prefix, str(target_year), str(get_uid(min_uid, max_uid, uids))])

# TODO
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
