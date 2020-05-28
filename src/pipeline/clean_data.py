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

# Concatenate address columns into full_address column
def create_column_full_address(data):

    # Truncate suffix_direction to first letter (N, S, E, W)
    data['suffix_direction'] = data['suffix_direction'].str[0].fillna('')

    # Convert zip_code to string
    data['zip_code'] = data['zip_code'].fillna('').astype(str)

    # Combine address columns to concatenate
    address_columns = ["address_start", "street_direction", "street_name", "street_suffix", "suffix_direction",
                      "zip_code"]

    # Concatenate address values
    data['full_address'] = data[address_columns].fillna('').astype(str).apply(' '.join, axis=1).str.replace('  ', ' ')

    # Replace empty strings with NaN values
    data[address_columns] = data[address_columns].replace('', np.nan)
    
    return data

def geocode_latitude_longitude(data):
    
    # Extract rows missing in latitude_longitude
    data_missing = data[data['latitude_longitude'].isnull()==1]
    
    # How many rows are missing coordinates
    num_missing = len(data_missing)

    # Create helper function to geocode missing latitude_longitude values
    def geocoder(address, key, agent, timeout=5):

        """
        Uses GoogleMaps API to batch geocode address strings to lat/long coordinates. RateLimiter is to 
        avoid timeout errors. If an address cannot be geocoded it is left as NaN. Use of GoogleMaps 
        API incurs a charge at $0.005 per request.

        """

        if address:
            # Instantiates GoogleMaps geocoder
            geolocator = GoogleV3(api_key=key, 
                                  user_agent=agent, 
                                  timeout=timeout)

            # Adds Rate Limiter to space out requests
            geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
            location = geolocator.geocode(address)
            latitude, longitude = location.latitude, location.longitude

            return latitude, longitude
        else:
            return np.nan
        
    # Calculate cost
    cost = num_missing * 0.005
    print("Cost for geocoding {} addresses is ${:.2f}.".format(num_missing, cost))

    # Geocode missing coordinates using full addresses
    if len(data_missing) > 0:
        try:
            print("Geocoding...")
            data_missing['latitude_longitude'] = data_missing['full_address'].apply(geocoder, args=(GOOGLE_API_KEY,
                                                                                                    GOOGLE_AGENT))
            print("{} locations were assigned coordinates.".format(num_missing))
        except Exceptions as e:
            print("Error:\n", e)
    else:
        print("No missing coordinates.")
        return data

    # Update dataframe
    return data.update(data_missing)

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

    # Fetch data
    data = fetch_data(sql, conn, date_columns)

    # Concatenate and create full_address
    data = create_column_full_address(data)

    # Geocode
    data = geocode_latitude_longitude(data)