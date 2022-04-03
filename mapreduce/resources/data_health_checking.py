import logging
import os
import sys
from multiprocessing import Pool

import pandas as pd


def check_missing_month_statistics(filepath):
    print(filepath)
    df = pd.read_csv(filepath)
    df["valid"] = df["valid"].apply(lambda x: x[:7])
    missing_dates = pd.period_range(start='2012-01', end='2021-12', freq='M').difference(df["valid"])

    if missing_dates.size > 0:
        # print(filepath + " missing:\n")
        print(filepath + " missing:\n", missing_dates.values)
    else:
        print(filepath + ": no missing data")


if __name__ == "__main__":
    # set up logging
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

    # check argv
    try:
        directory = sys.argv[1]
        if directory[-1] != '/':
            directory += '/'
    except IndexError:
        logging.error('Missing argument "directory". Use: python data_health_checking.py directory')
        sys.exit(1)

    logging.info("Checking missing monthly statistics")
    files = [directory + f for f in os.listdir(directory) if os.path.isfile(directory + f)]
    with Pool() as pool:
        pool.map(check_missing_month_statistics, files)

    logging.info("Data health checking is done")


