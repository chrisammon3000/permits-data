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

# Get raw data column names
def get_table_names(table):
    sql = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{}'".format(table)
    etl = pd.read_sql_query(sql, conn)
    columns = etl['column_name']
    
    return columns

# Rename columns, will update table later
def format_names(series):
    
    replace_map = {' ': '_', '-': '_', '#': 'No', '/': '_', 
                   '.': '', '(': '', ')': '', "'": ''}

    def replace_chars(text):
        for oldchar, newchar in replace_map.items():
            text = text.replace(oldchar, newchar).lower()
        return text

    return series.apply(replace_chars)

# Creates a SQL query to update table columns and writes to text file
### pass conn context
def create_query(old_columns, new_columns, db_table, run=False, con=conn):
    
    sql = 'ALTER TABLE {} '.format(db_table) + 'RENAME "{old_name}" to {new_name};'
    
    sql_query = []

    for idx, name in old_columns.iteritems():
        #print(idx, name)
        sql_query.append(sql.format(old_name=name, new_name=new_columns[idx]))
        
    update_names = '\n'.join(sql_query)
    # update later: sql_file = os.path.join(os.path.dirname(__file__), "../postgres/scripts/update_names.sql")
    with open('../postgres/sql/update_names.sql', 'w') as text:
        text.write(update_names)
        
    if run:
        cur = conn.cursor()
        sql_file = open('../postgres/sql/update_names.sql', 'r')
        cur.execute(sql_file.read())
        conn.commit()
        #conn.close()

#############################

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

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

    # Environment variables specific to notebook
    DATA_DIR = os.path.dirname(root_dir) + '/data'
    DB_TABLE = "permits_raw"

    # Retrieve table column names
    old_columns = get_table_names("permits_raw")

    # Transform table column names for permits_raw
    new_columns = format_names(old_columns)

    # Create SQL query for permits_raw
    try:
        create_query(old_columns, new_columns, run=True, con=conn, db_table=DB_TABLE)
    except: 
        conn.rollback()
        print("Query unsuccessful, try again.")


    main()
