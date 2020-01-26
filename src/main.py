import pandas as pd
import numpy as np
import json
import os
import logging
from pprint import pprint
from scipy.stats import expon
from typing import Dict, NewType
from config.config import DIR_DATA, DIR_CONFIG, DIR_OUTPUT
from util.helper import get_postal_code as gpc
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

def main():

    # o = pd.read_excel(input_file, sheet_name='Orders')
    # r = pd.read_excel(input_file, sheet_name='Returns')
    # p = pd.read_excel(input_file, sheet_name='People')

    logging.info(f'Reading in files from {DIR_DATA}...')
    df = pd.read_excel(os.path.join(DIR_DATA, 'Global Superstore.xls'))
    # Barb's original work; used to determine the needed columns
    o = pd.read_csv(os.path.join(DIR_DATA, 'orders.csv'))
    p = pd.read_csv(os.path.join(DIR_DATA, 'products.csv'))
    rg = pd.read_csv(os.path.join(DIR_DATA, 'regions.csv'))
    rt = pd.read_csv(os.path.join(DIR_DATA, 'returns.csv'))
    c = pd.read_csv(os.path.join(DIR_DATA, 'customers.csv'))
    df.columns = [i.replace(' ', '_').replace('-', '_').lower() for i in df.columns]

    # generate columns configuration
    conf = {}
    conf['cols'] = {}
    conf['cols']['orders'] = o.columns.tolist()
    conf['cols']['products'] = p.columns.tolist()
    conf['cols']['regions'] = rg.columns.tolist()
    conf['cols']['returns'] = rt.columns.tolist()
    conf['cols']['customers'] = c.columns.tolist()

    dfs = transform(df, conf)

    normalize(dfs)

    return

def transform(df: DataFrame, conf: Dict) -> DataFrame:

    logging.info(f'Beginning transformation...')
    out = df.copy()
    out['product_cost_to_consumer'] = get_product_cost_to_consumer(df)
    out['country_code'] = get_country_code(df)
    out['salesperson'] = get_salesperson(df)
    out['return_date'] = get_return_date(df)
    out['return_quantity'] = get_return_quantity(df)
    out['reason_returned'] = get_reason_returned(df)
    out['discount'] = get_discount(df)
    out['postal_code'] = get_postal_code(df)

    dfs = {k: out.loc[:, out.columns.intersection(v)].reindex()\
           for k, v in conf['cols'].items()}

    return dfs

def normalize(dfs: Dict[str, DataFrame]):

    logging.info(f'Beginning normalization...')
    normalize_orders(dfs['orders']).to_csv(os.path.join(DIR_OUTPUT, 'orders.csv'))
    normalize_products(dfs['products']).to_csv(os.path.join(DIR_OUTPUT, 'products.csv'))
    normalize_regions(dfs['regions'], gpc).to_csv(os.path.join(DIR_OUTPUT, 'regions.csv'))
    normalize_returns(dfs['returns']).to_csv(os.path.join(DIR_OUTPUT, 'returns.csv'))
    normalize_customers(dfs['customers']).to_csv(os.path.join(DIR_OUTPUT, 'customers.csv'))
    logging.info(f'Successfully wrote files to ./{DIR_OUTPUT}')

if __name__ == '__main__':

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p')
    main()
