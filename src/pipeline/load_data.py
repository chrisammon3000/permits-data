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

# Get raw data column names
def get_table_names(db_table, con):
    sql = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{}'".format(db_table)
    etl = pd.read_sql_query(sql, con)
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
# Creates a SQL query to update table columns and writes to text file
### add path string
def create_query(old_columns, new_columns, db_table, con, path, run=False):
    
    sql = 'ALTER TABLE {} '.format(db_table) + 'RENAME "{old_name}" to {new_name};'
    
    
    sql_query = []

    for idx, name in old_columns.iteritems():
        sql_query.append(sql.format(old_name=name, new_name=new_columns[idx]))
        
    update_names = '\n'.join(sql_query)
    
    # replace with path
    with open(path, 'w') as text:
        text.write(update_names)
        
    # Update db is desired
    if run:
        try:
            cur = con.cursor()
            print("Reading...")
            sql_file = open(path, 'r')
            print("Executing...")
            cur.execute(sql_file.read())
            con.commit()
            print("Closing connection...")
            #conn.close()
            print("Done.")
        except Exception as e:
            conn.rollback()
            print("Error: ", e)

def rename_columns(db_table, path, con):
    # Retrieve table column names
    old_columns = get_table_names(db_table, con)

    # Transform table column names for permits_raw
    new_columns = format_names(old_columns)

    #Create SQL query for permits_raw
    try:
        create_query(old_columns=old_columns, new_columns=new_columns, run=True, 
                    con=con, db_table=db_table, path=path)
        print("Table updated.")
    except Exception as e: 
        con.rollback()
        print("Query unsuccessful, try again.")
        print('Error:/n', e)

    # create_query(old_columns=old_columns, new_columns=new_columns, run=True, 
    #                 con=conn, db_table=DB_TABLE)
    # print("Table updated.")

    return get_table_names(db_table, con)


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

    sql_path = project_dir + '/postgres/sql/update_names.sql'

    print('Connecting...')
    try: 
        conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, 
                            host=DB_HOST, port=DB_PORT)
    except Exception as e:
        print('Unable to connect.')
        print('Error: ', e)

    print('Updating column names...')
    update = rename_columns(db_table=DB_TABLE, path=sql_path, con=conn)

    conn.close()
    print('Connection closed.')
