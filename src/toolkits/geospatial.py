import os
import sys
from pathlib import Path
sys.path[0] = str(Path(__file__).resolve().parents[2]) # Set path for modules
from dotenv import load_dotenv, find_dotenv
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'; turn off SettingWithCopyWarning
import psycopg2 # SQL libraries
from geopy.geocoders import Nominatim # Import dependencies for geocoding
from geopy.geocoders import GoogleV3
from geopy.extra.rate_limiter import RateLimiter

# find .env automagically by walking up directories until it's found, then
# load up the .env entries as environment variables
load_dotenv(find_dotenv());

# Google Maps environment variables
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_AGENT = os.getenv("permits-data")

# Create helper function to geocode missing latitude_longitude values
def geocode(address, key, agent, timeout=10):

    """
    Uses GoogleMaps API to batch geocode address strings to lat/long coordinates. RateLimiter is to 
    avoid timeout errors. If an address cannot be geocoded it is left as NaN. Use of GoogleMaps 
    API incurs a charge at $0.005 per request.

    """

    if address:
        # Instantiates GoogleMaps geocoder
        geolocator = GoogleV3(api_key=key or key, 
                                user_agent=agent or agent, 
                                timeout=timeout)

        # Adds Rate Limiter to space out requests
        geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
        location = geolocator.geocode(address)
        latitude, longitude = location.latitude, location.longitude

        return latitude, longitude
    else:
        return np.nan

# Takes addresses and outputs coordinates
def geocode_from_address(data, key=None, agent=None):
    
    # Extract rows missing in latitude_longitude
    data_missing = data[data['latitude_longitude'].isnull()==1]
    
    # How many rows are missing coordinates
    num_missing = len(data_missing)

    # Calculate cost
    cost = num_missing * 0.005
    print("Cost for geocoding {} addresses is ${:.2f}.".format(num_missing, cost))

    # Google Maps environment variables
    load_dotenv(find_dotenv());
    key = os.getenv("GOOGLE_API_KEY") or key
    agent = os.getenv("GOOGLE_AGENT") or agent

    # Geocode missing coordinates using full addresses
    if len(data_missing) > 0:
        try:
            print("Geocoding...")
            data_missing.loc[:, 'latitude_longitude'] = data_missing['full_address'].apply(geocode, args=(key,
                                                                                                    agent))
            print("{} locations were assigned coordinates.".format(num_missing))
        except Exception as e:
            print("Error:\n", e)
    else:
        print("No missing coordinates.")
        return data

    # Update dataframe
    data.update(data_missing, overwrite=False)

    return