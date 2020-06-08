import os
import sys
from pathlib import Path
sys.path[0] = str(Path(__file__).resolve().parents[2]) # Set path for modules
from dotenv import load_dotenv, find_dotenv
import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'; turn off SettingWithCopyWarning


# Concatenate address columns into full_address column
def create_full_address(data):

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
    if data['latitude_longitude'].isnull().any(): 
        raise AssertionError("Missing coordinates must be geocoded.")

    # Split latitude_longitude into separate columns and convert to float values: latitude, longitude
    if ['latitude', 'longitude'] not in data.columns.tolist():
        lat_long_series = data['latitude_longitude'].astype(str).str[1:-1].str.split(',', expand=True) \
                            .astype(float).rename(columns={0: "latitude", 1: "longitude"})

        # Add to original data
        ### Add drop duplicate columns
        return pd.concat([data, lat_long_series], axis=1)
    
    else:
        return None
