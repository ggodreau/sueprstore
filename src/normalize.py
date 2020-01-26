import sys
import logging
import pandas as pd
from typing import Dict, NewType

DataFrame = NewType('DataFrame', pd.DataFrame)
Series = NewType('Series', pd.Series)

########
# Orders
########

def normalize_orders(df: DataFrame) -> DataFrame:
    logging.debug(f'Normalizing data in {sys._getframe(  ).f_code.co_name}...')
    df.index.name = 'id'
    logging.debug(f'Returned {df.shape[0]} records in {sys._getframe(  ).f_code.co_name}...')
    return df

##########
# Products
##########


def normalize_products(df: DataFrame) -> DataFrame:
    logging.debug(f'Normalizing data in {sys._getframe(  ).f_code.co_name}...')
    res: Dict[str, list] = {}
    cols: List[str] = df.columns
    # descend into groupby object
    for r in df.copy().groupby('product_id'):
        # iterate through nested series object
        for sr in r[1].iterrows():
            # if cost to consumer isn't null, update res key
            if not pd.isnull(sr[1].loc['product_cost_to_consumer']):
                res[r[0]] = sr[1].tolist()
    out_df = pd.DataFrame(data=res).T
    out_df.columns = cols
    out_df.set_index('product_id', inplace=True)
    logging.debug(f'Returned {out_df.shape[0]} records in {sys._getframe(  ).f_code.co_name}...')
    return out_df

#########
# Regions
#########

def normalize_regions(df: DataFrame, gpc) -> DataFrame:
    logging.debug(f'Normalizing data in {sys._getframe(  ).f_code.co_name}...')
    res = df.copy().groupby(['region', 'country', 'state',\
                             'city', 'salesperson']).max().reset_index()
    res['postal_code'] = gpc(res)
    res.set_index('region', inplace=True)
    logging.debug(f'Returned {res.shape[0]} records in {sys._getframe(  ).f_code.co_name}...')
    return res

#########
# Returns
#########

def normalize_returns(df: DataFrame) -> DataFrame:
    logging.debug(f'Normalizing data in {sys._getframe(  ).f_code.co_name}...')
    res: Dict[str, list] = {}
    cols: List[str] = df.columns
    # descend into groupby object
    for r in df.copy().groupby('order_id'):
        # iterate through nested series object
        for sr in r[1].iterrows():
            # singleton return row
            res[r[0]] = sr[1].tolist()
    out_df = pd.DataFrame(data=res).T
    out_df.columns = cols
    out_df.set_index('order_id', inplace=True)
    # return only rows where return qty > 0
    out_df = out_df.copy()[ out_df['return_quantity']  > 0 ]
    logging.debug(f'Returned {out_df.shape[0]} records in {sys._getframe(  ).f_code.co_name}...')
    return out_df

###########
# Customers
###########

def normalize_customers(df: DataFrame) -> DataFrame:
    logging.debug(f'Normalizing data in {sys._getframe(  ).f_code.co_name}...')
    res: Dict[str, list] = {}
    cols: List[str] = df.columns
    # descend into groupby object
    for r in df.copy().groupby('customer_id'):
        # iterate through nested series object
        for sr in r[1].iterrows():
            # singleton return row
            res[r[0]] = sr[1].tolist()
    out_df = pd.DataFrame(data=res).T
    out_df.columns = cols
    out_df.set_index('customer_id', inplace=True)
    # drop postal_code because this is duplicated in regions df
    out_df.drop(labels=['postal_code'], axis=1, inplace=True)
    logging.debug(f'Returned {out_df.shape[0]} records in {sys._getframe(  ).f_code.co_name}...')
    return out_df
