import os
import sys
from dotenv import load_dotenv, find_dotenv
import numpy as np
import pandas as pd
import psycopg2

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
        return None

    
    return con


# Fetch data from postgres
def fetch_data(sql, con):
    
    # Fetch fresh data
    data = pd.read_sql_query(sql, con, coerce_float=False)

    # Replace None with np.nan
    data.fillna(np.nan, inplace=True)
    
    return data
                

## Get raw data column names
def get_table_names(table, con):
    sql = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{}'".format(table)
    etl = pd.read_sql_query(sql, con)
    columns = etl['column_name']
    
    return columns


def compare_column_order(data, db_table, con, match_inplace=False):
    
    db_columns = get_table_names(db_table, con).tolist()
    data_columns = data.columns.tolist()
    
    # Debugging
    #print(db_columns)
    #print(data_columns)

    if match_inplace:
        columns_reordered = get_table_names(db_table, con).tolist()
        data = data[columns_reordered]
        data_columns = data.columns.tolist()
    
    if set(db_columns) == set(data_columns):
        print('All columns in dataframe are in table "{}".'.format(db_table))
        if db_columns == data_columns:
            print('The current order of dataframe columns is identical to table "{}".'.format(db_table))
            if not match_inplace:
                return True
            else:
                return data
        else:
            print('The current order of dataframe columns is NOT identical to table "{}".'.format(db_table))
            return False

    elif len(set(db_columns)) > len(set(data_columns)):
        print('Current dataframe has less columns than table "{}":\n'.format(db_table), 
                                        list(set(db_columns) - set(data_columns)))
        return False
    else:
        print('Current dataframe has more columns than table "{}":\n.'.format(db_table), 
                                        list(set(data_columns) - set(db_columns)))
        return False


    # if db_columns == data_columns:
    #     print('The current order of columns is identical to table "{}".'.format(db_table))
    #     return True
    # else:
    #     if set(db_columns) == set(data_columns):
    #         print('Columns are the same as table "{}" but the order is incorrect.'.format(db_table))
    #         return False
    #     else:
    #         if len(set(db_columns)) > len(set(data_columns)):
    #             print('Current data has less columns than table "{}":\n.'.format(db_table),
    #                  list(set(db_columns) - set(data_columns)))
    #             return False
    #         else:
    #             print('Current data has more columns than table "{}":\n.'.format(db_table),
    #                  list(set(data_columns) - set(db_columns)))

    return data


def add_columns(data, db_table, con, run=False):

    # Get names of current columns in PostgreSQL table
    current_names = get_table_names(db_table, con)

    # Get names of updated table not in current table
    updated_names = data.columns.tolist()
    new_names = list(set(updated_names) - set(current_names))
    
    # Check names list is not empty
    if not new_names:
        print("Table is up to date.")
        return

    # Format strings for query
    alter_table_sql = "ALTER TABLE {db_table}\n"
    add_column_sql = "\tADD COLUMN {column} TEXT,\n"

    # Create a list and append ADD column statements
    sql_query = [alter_table_sql.format(db_table=db_table)]
    for name in new_names:
        sql_query.append(add_column_sql.format(column=name))

    # Join into one string
    sql_query = ''.join(sql_query)[:-2] + ";"
    
    if run:
        ### ADD TRY/EXCEPT TO RUN QUERY AGAINST DB
        try:
            print("Connecting...")
            cur = con.cursor()
            print("Executing query...")
            cur.execute(sql_query)
            print("Committing changes...")
            con.commit()
            cur.close()
            print("Database updated successfully:\nAdd columns {}".format(', '.join(new_names)))
        except Exception as e:
            conn.rollback()
            print('Error:\n', e)
    
    return sql_query


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
def update_table_names(old_columns, new_columns, db_table, con, path, run=False):
    
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
            print('Executing update query on table "{}"...'.format(db_table))
            cur.execute(sql_file.read())
            con.commit()
            cur.close()
            print("Table is updated.")
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


    # Builds a query to update postgres from a csv file
def update_table_values(db_table, con, data_path, sql_path, run=False):

    # CREATE TABLE and COPY
    tmp_table = "tmp_" + db_table
    
    column_names = get_table_names(db_table, con)
    column_names = column_names.tolist()
    names = ',\n\t'.join(['{}'.format(name) + " TEXT" for name in column_names])

    create_tmp_table_sql = 'CREATE TABLE {tmp_table} (\n\t{names}\n);\n\n'.format(tmp_table=tmp_table, names=names)
    copy_from_table_sql = "COPY {tmp_table} FROM \'{data_path}\' (FORMAT csv, HEADER TRUE);\n\nUPDATE {db_table}\n".format(tmp_table=tmp_table, 
                                                                                                   data_path=data_path, db_table=db_table)

    # SET statements
    updates_sql = ["SET "]

    for name in column_names:
        original_name = '{}'.format(name)
        set_sql = "{name} = {tmp_name},\n\t".format(name=original_name, 
                                                   tmp_name=tmp_table + '.' + name)
        updates_sql.append(set_sql)

    updates_sql = ''.join(updates_sql)

    updates_sql = updates_sql[:-3] + "\n"

    # FROM and WHERE clause
    tail_sql = "FROM {tmp_table}\nWHERE {db_table}.pcis_permit_no = {tmp_table}.pcis_permit_no;\n" \
        .format(tmp_table=tmp_table, db_table=db_table)

    sql_query = create_tmp_table_sql + copy_from_table_sql + updates_sql + tail_sql
    
    if run:
        ### ADD TRY/EXCEPT TO RUN QUERY AGAINST DB
        try:
            cur = con.cursor()
            print("Executing...")
            cur.execute(sql_query)
            con.commit()
            cur.close()
            print('Table "{}" is updated.'.format(db_table))
        except Exception as e:
            con.rollback()
            print('Error:\n', e)
    
    return sql_query


# Save csv with option to match order of columns in postgres
def save_csv(data, path, match_db_order=False, db_table=None, con=None):

    # Check unique columns
    assert data.columns.tolist() == data.columns.unique().tolist(), "Extra columns detected."
    
    # Check for null values
    assert data['latitude'].any(), 'Column "latitude" has missing values.'
    assert data['longitude'].any(), 'Column "longitude" has missing values.'

    # Check for erroneous coordinates. All coordinates should fall within Los Angeles county.
    assert (data['latitude'] > 33.2).all() and (data['latitude'] < 34.9).all(), "Incorrect latitude detected"
    assert (data['longitude'] > -118.9).all() and (data['longitude'] < -118).all(), "Incorrect longitude detected"

    if match_db_order:
        # Fetch names in postgres table and use to reorder columns dataframe
        columns_reordered = get_table_names(db_table, con).tolist()
        data = data[columns_reordered]
        print("Columns are have been reordered to match database table {}.".format(db_table))
    
    # Write to csv
    data.to_csv(path, index=False)
    
    return