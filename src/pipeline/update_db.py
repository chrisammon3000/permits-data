# -*- coding: utf-8 -*-
import os
import sys
from pathlib import Path
sys.path[0] = str(Path(__file__).resolve().parents[2]) # Set path for modules
import click
import logging
from dotenv import find_dotenv, load_dotenv
import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'; turn off SettingWithCopyWarning
import psycopg2
from src.toolkits.sql import connect_db, get_table_names, format_names, update_table_names # Import custom sql functions







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

    # Set environment variables
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    DB_PORT = os.getenv("DB_PORT")
    DB_HOST = os.getenv("DB_HOST")
    DATA_URL = os.getenv("DATA_URL")

    DB_TABLE = "permits_raw"