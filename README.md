# Superstore Transformer

Script to transform the [2017/2018 Superstore Tableau
dataset](https://community.tableau.com/thread/316509) into a 5-table
pseudo-normalized star-schema database. The script serves two purposes:

1. To move from a flat OLAP table to an OLTP table for joining and SQL exercises
1. To serve as a document of transformations from the source data set should we
   need to correct errors and/or modify the resultant set in the future

The dataset originally has 51,290 rows. In its transformed state (no bootstrapping), it has the following resultant tables:

- Orders: 51,290 rows (transaction hub / fact table)
- Products: 10,292 rows
- Regions: 3,189 rows
- Returns: 4,642 rows
- Customers: 1,590 rows

This utility has the capability to bootstrap a configurable number of records with parameterized randomness. In other words, you can generate as many additional rows as you want, and the script is designed to keep equal weighting and distributions to the source dataset, effectively making the dataset more 'dense', without affecting trends, etc. Additionally, you can configure the amount of randomness you want on a per-feature basis, such as delays between order and ship date and the per-year discount trends. This can be configured within `src/config/bootstrap.json`.

## Dependencies

You will need Python 3.5+, pandas, and scipy. Please be sure `which python`
returns Python 3.5+.

## Folder Structure

- `src` - transformation functions
- `src/data` - contains _input_ data files, do not modify
- `src/out` - contains generated _output_ bootstrap shards and csv files to be used in curriculum
- `src/util` - helper utilities common to root src files
- `src/config` - configuration and lookup files used during transformation

## Configuration

In `src/config` you will see the following configuration files:

- `bootstrap.json` - this sets interpolation parameters during bootstrapping (i.e. number of resultant rows, tolerances for randomness, discount curves, etc.)
- `config.py` - this contains global variables for relative directory links
- `country_codes.json` - this contains a lookup of country codes and their ISO codes
- `regions.json` - this contains a list of sales reps and their territories

The most likely parameter you are looking for is in `bootstrap.json` and is the `desired_output_rows` field. This is the total number of rows in the `orders` transaction table that the bootstrapping will generate. It defaults to 1M rows and takes about 1h15m to complete in a single process on a 2018MBP.

## Running the Script

To bootstrap derive tables, clone this repository and enter the root directory. From there, run:

```
chmod +x ./run.sh && ./run.sh
```

From there, you can `tail -f out.log` to see the following:

```
02/01/2020 01:19:37 PM Transforming 51290 records in get_salesperson...
02/01/2020 01:19:37 PM Transforming 51290 records in get_return_date...
02/01/2020 01:19:37 PM Transforming 51290 records in get_return_quantity...
02/01/2020 01:19:46 PM Transforming 51290 records in get_reason_returned...
02/01/2020 01:19:52 PM Transforming 51290 records in get_discount...
02/01/2020 01:19:57 PM Transforming 51290 records in get_postal_code...
02/01/2020 01:19:57 PM Finished 'transform' in 25.5192 secs
02/01/2020 01:19:57 PM Beginning bootstrap...
02/01/2020 01:19:57 PM Interpolating data in interpolate...
02/01/2020 01:19:58 PM UID max: 5994252 min: 0 diff: 5942962 interpolate...
02/01/2020 01:20:35 PM Bootstrapping record number 60000
02/01/2020 01:21:16 PM Bootstrapping record number 70000
02/01/2020 01:22:00 PM Bootstrapping record number 80000
02/01/2020 01:22:41 PM Bootstrapping record number 90000
02/01/2020 01:23:23 PM Bootstrapping record number 100000
02/01/2020 01:24:05 PM Bootstrapping record number 110000

...

02/01/2020 02:26:03 PM Finished 'bootstrap' in 3965.3524 secs
02/01/2020 02:26:03 PM Beginning column selection...
02/01/2020 02:26:03 PM Finished 'select_columns' in 0.5843 secs
02/01/2020 02:26:03 PM Beginning normalization...
02/01/2020 02:26:03 PM Normalizing data in normalize_orders...
02/01/2020 02:26:03 PM Returned 1000000 records in normalize_orders...
02/01/2020 02:26:10 PM Normalizing data in normalize_products...
02/01/2020 02:28:17 PM Returned 10292 records in normalize_products...
02/01/2020 02:28:17 PM Normalizing data in normalize_regions...
02/01/2020 02:28:19 PM Retrieving postal code in get_postal_code...
02/01/2020 02:28:19 PM Returned 3819 records in normalize_regions...
02/01/2020 02:28:19 PM Normalizing data in normalize_returns...
02/01/2020 02:35:28 PM Returned 210380 records in normalize_returns...
02/01/2020 02:35:29 PM Normalizing data in normalize_customers...
02/01/2020 02:37:01 PM Returned 1590 records in normalize_customers...
02/01/2020 02:37:01 PM Successfully wrote files to ./src/out
02/01/2020 02:37:01 PM Finished 'normalize' in 657.7731 secs
02/01/2020 02:37:01 PM Finished 'main' in 4652.9932 secs
```

After that, your files should be written to `src/out` as a series of `.csv` files:

- `bootshard_123xx.csv` - these are shards of the bootstrap process. Should the bootstrapping terminate early for some reason, you can pick up where you left off with these files if needed.
- `bootshard_compiled.csv` - this is the raw, fully joined, non-normalized output of bootstrapping. This is the csv that then gets processed through the normalize function to create independent tables.
- Here are the bootstrapped, normalized output csv which is ready to load into a database and be used in curriculum:
  - `orders.csv`
  - `customers.csv`
  - `products.csv`
  - `regions.csv`
  - `returns.csv`
