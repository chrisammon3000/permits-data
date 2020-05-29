import os
import sys
from pathlib import Path
sys.path[0] = str(Path(__file__).resolve().parents[2]) # Set path for modules
import logging
from dotenv import load_dotenv, find_dotenv
import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'; turn off SettingWithCopyWarning
import psycopg2 # SQL libraries
from src.toolkits.sql import connect_db # Import custom sql functions
from geopy.geocoders import Nominatim # Import dependencies for geocoding
from geopy.geocoders import GoogleV3
from geopy.extra.rate_limiter import RateLimiter

# Fetch data from postgres
def fetch_data(sql, con):
    
    # Fetch fresh data
    data = pd.read_sql_query(sql, con, coerce_float=False)

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
            data_missing.loc[:, 'latitude_longitude'] = data_missing['full_address'].apply(geocoder, args=(GOOGLE_API_KEY,
                                                                                                    GOOGLE_AGENT))
            print("{} locations were assigned coordinates.".format(num_missing))
        except Exception as e:
            print("Error:\n", e)
    else:
        print("No missing coordinates.")
        return data

    # Update dataframe
    data.update(data_missing)

    return data

def split_column_lat_long(data):
    
    # Check that there are no more missing coordinates before proceeding
    assert data['latitude_longitude'].notnull().any(), "Missing coordinates must be geocoded."

    # Split latitude_longitude into separate columns and convert to float values: latitude, longitude
    if ['latitude', 'longitude'] not in data.columns.tolist():
        lat_long_series = data['latitude_longitude'].astype(str).str[1:-1].str.split(',', expand=True) \
                            .astype(float).rename(columns={0: "latitude", 1: "longitude"})

        # Add to original data
        return pd.concat([data, lat_long_series], axis=1)

# Checks columns are correct and saves to csv
def save_csv(data, path):

    # Check unique columns
    assert data.columns.tolist() == data.columns.unique().tolist(), "Extra columns detected."
    
    # Check for null values
    assert data['latitude'].any(), 'Column "latitude" has missing values.'
    assert data['longitude'].any(), 'Column "longitude" has missing values.'

    # Check for erroneous coordinates. All coordinates should fall within Los Angeles county.
    assert (data['latitude'] > 33.2).all() and (data['latitude'] < 34.9).all(), "Incorrect latitude detected"
    assert (data['longitude'] > -118.9).all() and (data['longitude'] < -118).all(), "Incorrect longitude detected"

    # Write to csv
    data.to_csv(path, index=False)
    
    return

def main():
    # Connect to db
    conn = connect_db()

    # SQL query to extract partial dataset
    sql = 'SELECT * FROM {} LIMIT 500;'.format(DB_TABLE)

    # Path to csv
    save_path = project_dir + '/data/interim/permits_geocoded.csv'

    # Fetch data
    data = fetch_data(sql, conn)
    #print(data.head())

    # Concatenate and create full_address
    data = create_column_full_address(data)
    #print(data['full_address'])

    # Geocode
    data = geocode_latitude_longitude(data)

    # Split latitude_longitude into separate columns and convert to float values: latitude, longitude
    data = split_column_lat_long(data)
    print("\nNew columns created:\n\n", data[['latitude_longitude', 'latitude', 'longitude', 'full_address']].head(), "\n")

    # Save to interim folder
    print("Saving to interim folder...")
    save_csv(data, save_path)

    conn.close()
    print("Connection closed.")

    return

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    # only works in Python 3.6.1 and above
    # Get project root directory
    project_dir = sys.path[0]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv());
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    DB_PORT = os.getenv("DB_PORT")
    DB_HOST = os.getenv("DB_HOST")
    DATA_URL = os.getenv("DATA_URL")

    # Google Maps environment variables
    GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
    GOOGLE_AGENT = "permits-data"
    

    # Environment variables specific to notebook
    DATA_DIR = project_dir + '/data'
    DB_TABLE = "permits_raw"

    main()