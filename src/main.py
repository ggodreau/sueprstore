import pandas as pd
import numpy as np
import json
import os
import logging
from scipy.stats import expon
from typing import Dict, NewType, Any
from config.config import DIR_DATA, DIR_CONFIG, DIR_OUTPUT
from util.helper import get_postal_code as gpc
from util.helper import timer
from bootstrap import interpolate
from transform import *
from normalize import *

DataFrame = NewType('DataFrame', pd.DataFrame)
Series = NewType('Series', pd.Series)

'''
orders table - large, spanning multiple years
returns table - subset of orders table, include additional fake columns
regions table - includes geographic data
customers table - info about all customers (including some that did not place orders)
products table - info about products, including some that were not ordered.

Superstore xls pulled from here: https://community.tableau.com/thread/316509
'''

@timer
def main():
    '''
    Transformation pipeline
    '''
    # o = pd.read_excel(input_file, sheet_name='Orders')
    # r = pd.read_excel(input_file, sheet_name='Returns')
    # p = pd.read_excel(input_file, sheet_name='People')

    logging.info(f'Reading in files from {DIR_DATA}...')
    df = pd.read_excel(os.path.join(DIR_DATA, 'Global Superstore.xls'),
                       dtype={
                           'Order Date': 'str',
                           'Ship Date': 'str'
                       })

    # Barb's original work; used to determine the needed columns
    o = pd.read_csv(os.path.join(DIR_DATA, 'orders.csv'))
    p = pd.read_csv(os.path.join(DIR_DATA, 'products.csv'))
    rg = pd.read_csv(os.path.join(DIR_DATA, 'regions.csv'))
    rt = pd.read_csv(os.path.join(DIR_DATA, 'returns.csv'))
    c = pd.read_csv(os.path.join(DIR_DATA, 'customers.csv'))
    df.columns = [i.replace(' ', '_').replace('-', '_').lower() for i in df.columns]

    # generate columns configuration
    conf_cols = {}
    conf_cols['cols'] = {}
    conf_cols['cols']['orders'] = o.columns.tolist()
    conf_cols['cols']['products'] = p.columns.tolist()
    conf_cols['cols']['regions'] = rg.columns.tolist()
    conf_cols['cols']['returns'] = rt.columns.tolist()
    conf_cols['cols']['customers'] = c.columns.tolist()

    with open(os.path.join(DIR_CONFIG, 'transform.json')) as f:
        conf_transform: Dict[Any, Any] = json.loads(f.read())
    df_transformed = transform(df, conf_transform)
    with open(os.path.join(DIR_CONFIG, 'bootstrap.json')) as f:
        conf_boot: Dict[Any, Any] = json.loads(f.read())
    df_bootstrapped = bootstrap(df_transformed, conf_boot)
    df_dict = select_columns(df_bootstrapped, conf_cols)
    normalize(df_dict)

    return

@timer
def transform(df: DataFrame, conf: Dict[Any, Any]) -> DataFrame:
    '''
    Modification of fields in-place
    '''
    logging.info(f'Beginning transformation...')
    out = df.copy()

    out['order_id'] = get_shifted_order_id(df, conf)
    out['order_date'] = get_shifted_order_date(df, conf)
    out['ship_date'] = get_shifted_ship_date(df, conf)
    out['product_cost_to_consumer'] = get_product_cost_to_consumer(df)
    out['country_code'] = get_country_code(df)
    out['salesperson'] = get_salesperson(df)
    out['return_date'] = get_return_date(out, conf)
    out['return_quantity'] = get_return_quantity(df)
    out['reason_returned'] = get_reason_returned(df)
    out['discount'] = get_discount(df)
    out['postal_code'] = get_postal_code(df)

    return out

@timer
def bootstrap(df: DataFrame, conf: Dict[Any, Any]) -> DataFrame:
    '''
    Creation of new data
    '''
    logging.info(f'Beginning bootstrap...')
    return interpolate(df, conf)

@timer
def select_columns(df: DataFrame, conf: Dict) -> DataFrame:
    '''
    Cull output columns
    '''
    logging.info(f'Beginning column selection...')
    out = df.copy()
    dfs = {k: out.loc[:, out.columns.intersection(v)].reindex()\
           for k, v in conf['cols'].items()}

    return dfs

@timer
def normalize(dfs: Dict[str, DataFrame]):
    '''
    Normalize and link tables
    '''
    logging.info(f'Beginning normalization...')
    # generate regions and regions join key to orders
    regions, cjk = normalize_regions(dfs['regions'], gpc)
    regions.to_csv(os.path.join(DIR_OUTPUT, 'regions.csv'), float_format='%.0f')
    # pass regions join key into orders normalization
    normalize_orders(dfs['orders'], cjk).to_csv(os.path.join(DIR_OUTPUT, 'orders.csv'), float_format='%.2f')
    dfs['orders'].to_csv('./tmp_orders.csv')
    normalize_products(dfs['products']).to_csv(os.path.join(DIR_OUTPUT, 'products.csv'), float_format='%.2f')
    dfs['regions'].to_csv('./tmp_regions.csv')
    normalize_returns(dfs['returns']).to_csv(os.path.join(DIR_OUTPUT, 'returns.csv'))
    normalize_customers(dfs['customers']).to_csv(os.path.join(DIR_OUTPUT, 'customers.csv'))
    logging.info(f'Successfully wrote files to ./{DIR_OUTPUT}')

if __name__ == '__main__':

    logging.basicConfig(
        level=logging.DEBUG,
        filename='out.log',
        format='%(asctime)s %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p')
    main()
