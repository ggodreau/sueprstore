import pandas as pd


'''
orders table - large, spanning multiple years
returns table - subset of orders table, include additional fake columns
regions table - includes geographic data
customers table - info about all customers (including some that did not place orders)
products table - info about products, including some that were not ordered.

Superstore xls pulled from here: https://community.tableau.com/thread/316509
'''

def main():

    input_file = './data/Global Superstore.xls'

    o = pd.read_excel(input_file, sheet_name='Orders')
    r = pd.read_excel(input_file, sheet_name='Returns')
    p = pd.read_excel(input_file, sheet_name='People')

    # < transformation functions here >

    # return processed csvs for loading into sql db
    return

if __name__ == '__main__':
    main()
