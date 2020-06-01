import os
import sys
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
import pandas as pd
import psycopg2

sys.path[0] = str(Path(__file__).resolve().parents[2]) # Set path for custom modules

# if modulename not in sys.modules: print...
load_dotenv(find_dotenv());

# Updates column types in PostgreSQL database
def update_table_types(column_dict, sql_string, db_table, printed=False, 
                       write_query=False, write_path=None, run_query=False, con=None):
    
    """
    Takes a sql statement to ALTER COLUMN types (string)
    and appends it to an ALTER TABLE statement and prints
    or returns a full string. Made for PostgreSQL.
    
    Example
    ---------
    # Dictionary of SQL types
    numeric_cols = {'valuation': 'NUMERIC(12, 2)'}
    
    # ALTER column statement as format string, do not end with ',' or ';'
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
        
    db_table : string
        Name of table in database
    
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
    sql_alter_table = "ALTER TABLE public.{db_table}\n\t".format(db_table=db_table)

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
    
    if write_query:
        with open(write_path, 'w') as text:
            text.write(sql_update_type)
        print("\nSQL written to:\n{}\n".format(write_path))
    
    if run_query:
        #assert con, "No connection to database."
        # try in case connection not open
        try:
            print("Connecting to database...")
            cur = con.cursor()
            print('Executing query on table "{}"...'.format(db_table))
            cur.execute(sql_update_type)
            print("Committing changes...")
            con.commit()
            cur.close()
            print("Database updated successfully.")
        except Exception as e:
            con.rollback()
            print('Error:\n', e)

        
    return sql_update_type

# Save csv with option to match order of columns in postgres
def save_csv(data, path, match_db_order=False, db_table=None, con=None):

    # Check unique columns
    assert data.columns.tolist() == data.columns.unique().tolist(), "Extra columns detected."
    
    # Check for null values
    assert data['latitude'].any(), 'Column "latitude" has missing values.'
    assert data['longitude'].any(), 'Column "longitude" has missing values.'

    # Check for erroneous coordinates. All coordinates should fall within Los Angeles county.
    assert (data['latitude'] > 33.2).all() and (data['latitude'] < 34.9).all(), "Incorrect latitude detected."
    assert (data['longitude'] > -118.9).all() and (data['longitude'] < -118).all(), "Incorrect longitude detected."

    if match_db_order:
        # Fetch names in postgres table and use to reorder columns dataframe
        columns_reordered = get_table_names(db_table, con).tolist()
        data = data[columns_reordered]
        print("Columns in dataframe have been reordered to match table {}.".format(db_table))
    
    # Write to csv
    data.to_csv(path, index=False)
    
    return