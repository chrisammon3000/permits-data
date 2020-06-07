# -*- coding: utf-8 -*-
import os
import sys
from pathlib import Path
sys.path[0] = str(Path(__file__).resolve().parents[2]) # Set path for custom modules
import warnings
from io import StringIO
from dotenv import load_dotenv, find_dotenv
import numpy as np
import pandas as pd
import psycopg2
from src.pipeline.dictionaries import types_dict, replace_map
from src.pipeline.transform_data import create_full_address, split_lat_long
from src.toolkits.geospatial import geocode_from_address
from src.toolkits.postgresql import Database, Table
from src.toolkits.eda import explore_value_counts


def main():

    return

    if __name__ == '__main__':

        # not used in this stub but often useful for finding various files

        # find .env automagically by walking up directories until it's found, then
        # load up the .env entries as environment variables
        #load_dotenv(find_dotenv())

        print(types_dict)

        print("Initializing table...")
        permits_raw = Table(name="permits_raw")
        print("Update names...")
        permits_raw.format_table_names(replace_map=replace_map, update=True)
        print("Update types...")
        permits_raw.update_types(types_dict)
        print("Fetching...")
        data = permits_raw.fetch_data()

        data = create_full_address(data)
        print("Geocoding...")
        geocode_from_address(data)

        data = split_lat_long(data)
        print("Updating...")
        permits_raw.update_values(data=data, id_col="pcis_permit_no", types_dict=types_dict)