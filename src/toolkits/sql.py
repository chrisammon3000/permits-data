import os
import sys
from dotenv import load_dotenv, find_dotenv
import numpy as np
import pandas as pd
import psycopg2

# Set environment variables
load_dotenv(find_dotenv());
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
DB_PORT = os.getenv("DB_PORT")
DB_HOST = os.getenv("DB_HOST")
DATA_URL = os.getenv("DATA_URL")

# Connect to PostgreSQL, useful only for notebook
def connect_db():
    
    """
    Connects to PostgreSQL database using psycopg2 driver. Same
    arguments as psycopg2.connect().

    Params
    --------
    dbname
    user
    password
    host
    port
    connect_timeout

    """

    try:
        con = psycopg2.connect(dbname=POSTGRES_DB,
                               user=POSTGRES_USER,
                               password=POSTGRES_PASSWORD,
                                host=DB_HOST, 
                                port=DB_PORT,
                              connect_timeout=3)
        print('Connected as user "{}" to database "{}" on http://{}:{}.'.format(POSTGRES_USER,POSTGRES_DB,
                                                           DB_HOST,DB_PORT))
              
    except Exception as e:
        print('Error:\n', e)
    
    return con

## Get raw data column names
def get_table_names(table, con):
    sql = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{}'".format(table)
    etl = pd.read_sql_query(sql, con)
    columns = etl['column_name']
    
    return columns

# Map of character replacements
replace_map = {' ': '_', '-': '_', '#': 'No', '/': '_', 
               '.': '', '(': '', ')': '', "'": ''}

# Rename columns, will update table later
def format_names(series, char_map):
    
    def replace_chars(text):
        for oldchar, newchar in char_map.items():
            text = text.replace(oldchar, newchar).lower()
        return text

    return series.apply(replace_chars)

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
            #print("Closing connection...")
            cur.close()
            print("Done.")
        except Exception as e:
            conn.rollback()
            print('Error:\n', e)

# Updates column types in PostgreSQL database
def update_table_types(column_dict, sql_string, table, printed=False, 
                       write=False, path=None, run=False, con=None):
    
    """
    Takes a sql statement to ALTER COLUMN types (string)
    and appends it to an ALTER TABLE statement and prints
    or returns a full string. Made for PostgreSQL.
    
    Example
    ---------
    # Dictionary of SQL types
    numeric_cols = {'valuation': 'NUMERIC(12, 2)'}
    
    # ALTER column statement as string, do not end with ',' or ';'
    sql_numeric = "ALTER {column} TYPE {col_type} USING {column}::numeric"
    
    # Writes a out text file to disk
    update_table_types(integer_cols, sql_integer, printed=True, 
                                        write=True, path='./sql')
    
    Output
    ---------
    >>> "ALTER TABLE public.permits_raw
            ALTER valuation TYPE NUMERIC(12, 2) USING valuation::numeric;"
            
    Parameters
    ----------
    column_dict : dictionary
        Dictionary in form {'column name': 'SQL datatype'}
    
    sql_string : string
        Must be a SQL query string in form:
        "ALTER {column} TYPE {col_type} USING {column}::numeric

    table : string
        Name of table in PostgreSQL database
    
    printed : boolean
        If True will output to console
    
    write : boolean
        If True will write to specified path
    
    path : string
        Path of directory to write text file to
        
    run : boolean
        If True, will run query in database.
        
    con : psycopg2 connection object
        Required if run = True
    """
    
    # Define SQL update queries
    sql_alter_table = "ALTER TABLE public.{db_table}\n\t".format(db_table=table)

    # Append comma, new line and tab
    sql_string = sql_string + ",\n\t"
    
    # Update types
    sql_update_type = []
    for column, col_type in column_dict.items():
        sql_update_type.append(sql_string.format(column=column, col_type=col_type))
    
    # Join strings to create full sql query
    sql_update_type = sql_alter_table + ''.join(sql_update_type)
    
    # Replace very last character with ";"
    sql_update_type = sql_update_type[:-3] + ";"
    
    if printed:
        print(sql_update_type)
    
    if write:
        with open(path, 'w') as text:
            text.write(sql_update_type)
        print("\nSQL written to:\n{}\n".format(path))
    
        if run:
            #assert con, "No connection to database."
            # try in case connection not open
            try:
                print("Connecting...")
                cur = con.cursor()
                sql_file = open(path, 'r')
                print("Executing query...")
                cur.execute(sql_file.read())
                print("Committing changes...")
                con.commit()
                cur.close()
                print("Database updated successfully.")
            except Exception as e:
                conn.rollback()
                print('Error:\n', e)
                
    elif run and not write:
        print('Set "write=True" and define path to run query from file.')
        
    return sql_update_type