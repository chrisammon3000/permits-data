import os
import sys
# Set path for modules
sys.path[0] = '../'
from dotenv import load_dotenv, find_dotenv
import numpy as np
import pandas as pd
# SQL libraries
import psycopg2
# Import custom eda and sql functions
from src.toolkits.eda import get_snapshot
from src.toolkits.sql import connect_db, get_table_names
# Import dependencies for geocoding
from geopy.geocoders import Nominatim
from geopy.geocoders import GoogleV3
from geopy.extra.rate_limiter import RateLimiter

# Fetch data from postgres
def fetch_data(sql, con, date_columns):
    
    # Fetch fresh data
    data = pd.read_sql_query(sql, conn, parse_dates=date_columns, coerce_float=False)

    # Replace None with np.nan
    data.fillna(np.nan, inplace=True)
    
    return data





if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    # only works in Python 3.6.1 and above
    # Get project root directory
    project_dir = str(Path(__file__).resolve().parents[2])

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())
    
    # Get project root directory
    root_dir = os.path.dirname(os.getcwd())

    # Set environment variables
    load_dotenv(find_dotenv());
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    DB_PORT = os.getenv("DB_PORT")
    DB_HOST = os.getenv("DB_HOST")
    DATA_URL = os.getenv("DATA_URL")

    # Google Maps environment variables
    GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

    # Environment variables specific to notebook
    DATA_DIR = os.path.dirname(root_dir) + '/data'
    DB_TABLE = "permits_raw"

    # Connect to db
    conn = connect_db()

    # Extract partial dataset
    sql = 'SELECT * FROM {} LIMIT 500;'.format(DB_TABLE)

    # Columns to parse as dates
    date_columns = ['status_date', 'issue_date', 'license_expiration_date']

    # Fetch data
    data = fetch_data(sql, conn, date_columns)