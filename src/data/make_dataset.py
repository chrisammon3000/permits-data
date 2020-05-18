# -*- coding: utf-8 -*-

import os
import sys
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
import numpy as np
import pandas as pd
import psycopg2

#############################

# Load environment variables from .env
load_dotenv(find_dotenv());

# Set path for modules
sys.path[0] = '../'

# Set environment variables
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
DB_PORT = os.getenv("DB_PORT")
DB_HOST = os.getenv("DB_HOST")
DATA_URL = os.getenv("DB_DATA_URLPORT")

# User gives file paths
@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
@click.argument('output_filepath', type=click.Path())
def main(input_filepath, output_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')

# Read in data
raw_data = 'permits_raw.csv'
DATA_PATH = sys.path[0] + 'data/raw/' + raw_data

# Connect to db
conn = psycopg2.connect(dbname=POSTGRES_DB,
                       user=POSTGRES_USER,
                       password=POSTGRES_PASSWORD,
                        host=DB_HOST, 
                        port=DB_PORT)

# Extract full dataset
#data = pd.read_sql_query(sql, conn)

# Get raw data column names
def get_table_names(table):
    sql = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{}'".format(table)
    etl = pd.read_sql_query(sql, conn)
    old_columns = etl['column_name']
    
    return old_columns

# Retrieve table column names
old_columns = get_table_names("permits_raw")

# Rename columns, will update table later
def format_names(series):
    # Replace whitespace with underscore
    series = series.str.replace(' ', '_')

    # Replace hyphen with underscore
    series = series.str.replace('-', '_')

    # Replace hashtag with No (short for number)
    series = series.str.replace('#', 'No')

    # Replace forward slash with underscore
    series = series.str.replace('/', '_')

    # Remove period
    series = series.str.replace('.', '')

    # Remove open parenthesis
    series = series.str.replace('(', '')

    # Remove closed parenthesis
    series = series.str.replace(')', '')

    # Remove apostrophe
    series = series.str.replace("'", '')
    
    return series.str.lower()

# Transform table column names for permits_raw
new_columns = format_names(old_columns);

# Creates a SQL query to update table columns and writes to text file
def create_query(old_columns, new_columns, run=False):
    
    sql = 'ALTER TABLE permits_raw RENAME "{old_name}" to {new_name};'
    
    sql_query = []

    for idx, name in old_columns.iteritems():
        #print(idx, name)
        sql_query.append(sql.format(old_name=name, new_name=new_columns[idx]))
        
    update_names = '\n'.join(sql_query)
    # update later: sql_file = os.path.join(os.path.dirname(__file__), "../postgres/scripts/update_names.sql")
    with open('../postgres/sql/update_names.sql', 'w') as text:
        text.write(update_names)
        
    if run==True:
        cur = conn.cursor()
        sql_file = open('../postgres/sql/update_names.sql', 'r')
        cur.execute(sql_file.read())

# Create SQL query for permits_raw
create_query(old_columns, new_columns, run=True)

# Extract full dataset
data = pd.read_sql_query(sql_all, conn)
data.head()

#############################

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
