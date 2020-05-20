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
def create_query(old_columns, new_columns, db_table, con, run=False):
    
    sql = 'ALTER TABLE {} '.format(db_table) + 'RENAME "{old_name}" to {new_name};'
    
    sql_query = []

    for idx, name in old_columns.iteritems():
        #print(idx, name)
        sql_query.append(sql.format(old_name=name, new_name=new_columns[idx]))
        
    update_names = '\n'.join(sql_query)
    # update later: sql_file = os.path.join(os.path.dirname(__file__), "../postgres/scripts/update_names.sql")
    with open(project_dir + '/postgres/sql/update_names.sql', 'w') as text:
        text.write(update_names)
    
    # Update db if desired
    if run:
        cur = con.cursor()
        sql_file = open(project_dir + '/postgres/sql/update_names.sql', 'r')
        cur.execute(sql_file.read())
        con.commit()

def rename_columns(db_table, con):
    # Retrieve table column names
    old_columns = get_table_names(db_table, con)

    # Transform table column names for permits_raw
    new_columns = format_names(old_columns)

    #Create SQL query for permits_raw
    try:
        create_query(old_columns=old_columns, new_columns=new_columns, run=True, 
                    con=con, db_table=db_table)
        print("Table updated.")
    except Exception as e: 
        con.rollback()
        print("Query unsuccessful, try again.")
        print('Error: ', e)

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

    print('Connecting...')
    try: 
        conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, 
                            host=DB_HOST, port=DB_PORT)
    except Exception as e:
        print('Unable to connect.')
        print('Error: ', e)

    update = rename_columns(DB_TABLE, conn)

    conn.close()
    print('Connection closed.')
    # sql_file = project_dir + "/postgres/sql/update_names.sql"
    # print(sql_file)
