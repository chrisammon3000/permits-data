import os
import sys
from pathlib import Path
sys.path[0] = str(Path(__file__).resolve().parents[2]) # Set path for modules
import logging
from dotenv import load_dotenv, find_dotenv
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'; turn off SettingWithCopyWarning
import psycopg2 # SQL libraries
from src.toolkits.sql import connect_db # Import custom sql functions

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



def split_lat_long(data):
    
    # Check that there are no more missing coordinates before proceeding
    assert data['latitude_longitude'].notnull().any(), "Missing coordinates must be geocoded."

    # Split latitude_longitude into separate columns and convert to float values: latitude, longitude
    if ['latitude', 'longitude'] not in data.columns.tolist():
        lat_long_series = data['latitude_longitude'].astype(str).str[1:-1].str.split(',', expand=True) \
                            .astype(float).rename(columns={0: "latitude", 1: "longitude"})

        # Add to original data
        return pd.concat([data, lat_long_series], axis=1)


def main():
    
    # Connect to db
    conn = connect_db()

    # SQL query to extract partial dataset
    sql = 'SELECT * FROM {} LIMIT 500;'.format(DB_TABLE)

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

    # Path to csv
    save_path = project_dir + '/data/interim/permits_geocoded.csv'

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