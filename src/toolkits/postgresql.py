import os
import sys
from pathlib import Path
sys.path[0] = str(Path(__file__).resolve().parents[2]) # Set path for custom modules
from dotenv import load_dotenv, find_dotenv
import numpy as np
import pandas as pd
import psycopg2
import warnings
from io import StringIO
from src.pipeline.dictionaries import types_dict, replace_map

# if modulename not in sys.modules: print...
load_dotenv(find_dotenv());


#### Database class ####
class Database():

    """
    Provides an interface to offer basic functionality for working
    with a PostgreSQL database including creating, listing and
    dropping tables. Tables can be manipulated further through the Table
    method which inherits from Database. Built on psycopg2.

    Example: Creating, listing and dropping tables
    --------

    # Assumes .env file is present
    db = Database()
    
    # Create a new table
    db.create_table(table_name="permits_raw", types=dict=types_dict, 
                    id_col="pcis_permit_no")
    
    # List tables in database
    db.list_tables()
    [ 'permits_raw' ]

    # Drop a table
    db.drop_table(table_name="permits_raw")


    Example: Accessing the connection method allows custom queries
    --------

    custom_sql = "SELECT COUNT(*) FROM table;"

    # Create a connection variable
    con = db._connect()
    
    cur = con.cursor()
    cur.execute(custom_sql)
    
    for record in cur:
        print(record)

    # Close connection
    cur.close()
    con.close()

    """

    def __init__(self, user="postgres", password="postgres",
                 dbname=None, host="localhost", port=5432):

        # Loaded from .env if not explicit
        self.user = os.getenv("POSTGRES_USER") or user
        self.password = os.getenv("POSTGRES_PASSWORD") or password
        self.dbname = os.getenv("POSTGRES_DB") or dbname
        self.host = os.getenv("DB_HOST") or host
        self.port = os.getenv("DB_PORT") or port
        
    def _connect(self):

        """
        Connects to PostgreSQL database using psycopg2 driver. Same
        arguments as psycopg2.connect(). Returns standard connection.

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
            con = psycopg2.connect(dbname=self.dbname,
                                   user=self.user,
                                   password=self.password,
                                    host=self.host, 
                                    port=self.port,
                                  connect_timeout=3)            
        except Exception as e:
            print('Error:', e)
            return None

        return con
    
    @property
    def _con(self):
        """
        Checks the status of a connection to database.

        Example
        -------
        db = Database()

        db._con

        """
        try:
            con = self._connect()
            print('Connected as user "{}" to database "{}" on http://{}:{}.'.format(self.user,self.dbname,
                                                               self.host,self.port))
            con.close()
        except Exception as e:
            con.rollback()
            print('Error:', e)
        finally:
            if con is not None:
                con.close()
                
    def _run_query(self, sql, msg=None):
        """
        Runs internal queries.
        """
        try:
            con = self._connect()
        except Exception as e:
            print("Error:", e)            
            
        try:
            cur = con.cursor()
            cur.execute(sql)
            con.commit()
            cur.close()
            print(msg)
        except Exception as e:
            con.rollback()
            print("Error:", e)
        finally:
            if con is not None:
                con.close()
        
        return

    def create_table(self, table_name, types_dict, id_col, columns=None):
        """
        Creates a new table. Requires name, dictionary of column names as keys
        and their PostgreSQL types as values, and an id column as primary key.
        Desired columns can be specified.

        Params
        ------
        table_name : string
            Name of table

        types_dict : dict
            Dictionary in form "column name": "PostgreSQL type" eg. 
            "permit_category": "VARCHAR(50)"

        id_col : string
            Primary key of table

        columns : list of strings
            List of columns to select from types_dict
        """
        
        # Append id_col to selected columns
        columns = None if not columns else set([id_col] + columns)
        
        # Subsets types_dict by columns argument and formats into string if no columns are specified
        types_dict = types_dict if not columns else {key:value for key, value in types_dict.items() if key in set(columns)}
        names = ',\n\t'.join(['{key} {val}'.format(key=key, val=val) for key, val in types_dict.items()])
        
        # Build queries
        sql = 'CREATE TABLE {table_name} (\n\t{names}\n);\n\n' \
                            .format(table_name=table_name, names=names)
        
        # Execute query
        self._run_query(sql, msg='Created table "{name}" in database "{dbname}".'.format(name=table_name, dbname=self.dbname))
        
        return self
    
    def drop_table(self, table_name):
        """
        Drops table from database.
        """

        # Build queries
        sql = 'DROP TABLE IF EXISTS {table_name};\n\n'.format(table_name=table_name)
        
        # Execute query
        self._run_query(sql, msg="Dropped table {}.".format(table_name))
        
        return self
    
    def _subset_types_dict(self, types_dict, columns):
        """
        Internal method to Table class.
        """

        columns = self.get_names().tolist() if not columns else columns
        types_dict = {key:value for key, value in types_dict.items() if key in set(columns)}
        
        return types_dict, columns

    def _create_temp_table(self, types_dict, id_col, columns=None):
        """
        Creates temporary table for update_values operation.
        """
        
        # Append id_col to selected columns
        columns = None if not columns else [id_col] + columns

        # CREATE TABLE query
        tmp_table = "tmp_" + self.table

        sql = """
        DROP TABLE IF EXISTS {tmp_table};
        CREATE TABLE {tmp_table} AS (SELECT * FROM {table}) WITH NO DATA;
        """.format(tmp_table=tmp_table, table=self.table)

        # Execute query
        self._run_query(sql, msg='Created temporary table "{}".'.format(tmp_table))
        
        return self
    
    # List tables
    def list_tables(self):
        
        sql = """
        SELECT tablename FROM pg_catalog.pg_tables
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema');
        """
        
        try:
            con = self._connect()
            cur = con.cursor()
            cur.execute(sql)
        except Exception as e:
            con.rollback()
            print("Error:", e)
            
        results = cur.fetchall()
        cur.close()
        
        tables = []
        
        for result in results:
            tables.append(*result)
            
        return tables
        

#### Table class ####
class Table(Database):

    """
    Provides functionality for manipulating a table in a PostgreSQL database
    and updating with transformed data from a pandas dataframe.

    Methods
    -------
    fetch_data() --> Returns a pandas dataframe of table
    get_names() --> Returns table column names
    get_types() --> Returns types dictionary in form "column name": "PostgreSQL type" 
    format_table_names() --> Standardizes column names 
    add_columns_from_data() --> Adds new columns from a pandas dataframe
    update_values() --> Updates rows from a pandas dataframe
    update_types() --> Updates column types from a dictionary in form "column name": "PostgreSQL type"

    Example 1: Preparing a database table
    -------
    from src.pipeline.dictionaries import types_dict, replace_map
    
    db = Database()

    # Create a new table
    db.create_table(table_name="permits_raw", types=dict=types_dict, 
                    id_col="pcis_permit_no")
    
    # Set up table for ETL
    permits_raw = Table(name=name, id_col=id_col)
    permits_raw.format_table_names(replace_map=replace_map, update=True) # Standardize names
    permits_raw.update_types(types_dict=types_dict) # Update datatypes
    
    Example 2: ETL workflow
    --------
    from src.pipeline.transform_data import create_full_address, split_lat_long
    from src.toolkits.geospatial import geocode_from_address

    # Extract
    data = permits_raw.fetch_data()

    # Transform
    data = create_full_address(data)
    geocode_from_address(data)
    data = split_lat_long(data)

    # Load
    permits_raw.update_values(data=data, id_col=id_col, types_dict=types_dict)   

    """

    def __init__(self, name, id_col, user="postgres", password="postgres",
                 dbname=None, host="localhost", port=5432):
        
        super().__init__(user, password, dbname, host, port)
        
        self.table = name
        self.id_col = id_col
        
        # Loaded from .env if not explicit
        self.user = os.getenv("POSTGRES_USER") or user
        self.password = os.getenv("POSTGRES_PASSWORD") or password
        self.dbname = os.getenv("POSTGRES_DB") or dbname
        self.host = os.getenv("DB_HOST") or host
        self.port = os.getenv("DB_PORT") or port
        self.columns = self.get_names().tolist

    # Connect to database
    def __connect(self):
        return super(Table, self)._connect()
    
    # Check info on connection
    def __con(self):
        return super(Table, self)._con
    
    # Run query
    def __run_query(self, sql, msg):
        return super(Table, self)._run_query(sql, msg)
    
    def __subset_types_dict(self, types_dict, columns):
        return super(Table, self)._subset_types_dict(types_dict, columns)

    def __create_temp_table(self, types_dict, id_col, columns):
        return super(Table, self)._create_temp_table(types_dict, id_col, columns)
    
    # Fetch data from sql query
    def fetch_data(self, sql=None, coerce_float=False, parse_dates=None):
        """
        Fetches data from PostgreSQL table. Tries to preserve NA values
        for integers within pandas Dataframe and uses np.nan
        for other dtypes.
        """
        
        sql = sql or "SELECT * FROM {};".format(self.table)
        
        con = self.__connect()
        
        # Fetch fresh data
        data = pd.read_sql_query(sql=sql, con=con, coerce_float=coerce_float, parse_dates=parse_dates)
        
        # Recast integer columns to preserve original types
        try: 
            update_dict = self.get_types(pandas_integers=True)
            update_dict = {k: v for k, v in update_dict.items() if v}
            data = data.astype(update_dict)
        except:
            warnings.warn('Dataframe dtypes may be incorrect.')
            pass
        
        # Replace None with np.nan
        data.fillna(np.nan, inplace=True)
        
        # Close db connection
        con.close()

        return data
    
    # Get names of column
    def get_names(self):
        """
        Returns names of columns in table.
        """
        
        # Specific query to retrieve table names
        sql = "SELECT * FROM information_schema.columns WHERE table_name = N'{}'".format(self.table)
        
        # Run query and extract
        try:
            con = self.__connect()
            data = pd.read_sql_query(sql, con)
            column_series = data['column_name']
            con.close()
        except Exception as e:
            print("Error:", e)
    
        return column_series

    # Get types of columns, returns dict
    def get_types(self, as_dataframe=False, pandas_integers=False):
        """
        Returns dictionary of data types eg., 
        { "column name": "VARCHAR(100)", ... }

        Params
        ------
        as_dataframe : bool
            Returns dataframe instead of dictionary

        pandas_integers : bool
            Returns only integer types if True. Useful for preserving nulls
            in integer dtypes in pandas Dataframe.
        """
        
        # Specific query to retrieve table names
        sql_to_sql = """
        SELECT column_name, 
        CASE 
            WHEN domain_name is not null then domain_name
            WHEN data_type='character varying' THEN 'varchar('||character_maximum_length||')'
            WHEN data_type='character' THEN 'char('||character_maximum_length||')'
            WHEN data_type='numeric' THEN 'numeric'
            ELSE data_type
        END AS type
        FROM information_schema.columns WHERE table_name = 'permits_raw';
        """
        
        sql_to_pandas = """
        SELECT column_name, 
        CASE 
            WHEN domain_name is not null then domain_name
            WHEN data_type='smallint' OR data_type='integer' THEN 'Int64'
        END AS type        
        FROM information_schema.columns WHERE table_name = 'permits_raw';
        """
        
        sql = sql_to_sql if not pandas_integers else sql_to_pandas
        
        # Run query and extract
        try:
            con = self.__connect()
            data = pd.read_sql_query(sql, con)
            con.close()
        except Exception as e:
            print("Error:", e)
        
        data['type'] = data['type'].str.upper() if not pandas_integers else data['type']
        
        if as_dataframe:
            return data
        
        types_dict = dict(zip(data['column_name'], data['type']))
        
        return types_dict
    
    # Update column names in db table
    def _update_table_names(self, series):
        """
        Generates a SQL query to change the names of table columns.
        """

        # Extract current columns in table
        old_columns = self.get_names()

        # Create list of reformatted columns to replace old columns 
        new_columns = series

        # SQL query string to change column names
        sql = 'ALTER TABLE {} '.format(self.table) + 'RENAME "{old_name}" to {new_name};'

        sql_query = []

        # Iterate through old column names and replace each with reformatted name 
        for idx, name in old_columns.iteritems():
            sql_query.append(sql.format(old_name=name, new_name=new_columns[idx]))

        # Join list to string
        sql_query = '\n'.join(sql_query)

        return sql_query
    
    # Standardize column names using dictionary of character replacements
    def format_table_names(self, replace_map, update=False):
        """
        Standardizes table names.

        Params
        -------
        replace_map : dict
            dictionary of characters to replace as key:value

        update : bool
            True to run update query against table

        """
        
        series = self.get_names()
        
        def replace_chars(text):
            for oldchar, newchar in replace_map.items():
                text = text.replace(oldchar, newchar).lower()
            return text
        
        series = series.apply(replace_chars)  
        
        if not update:
            warnings.warn('No changes made. Set "update=True" to run query on database.')
            return series.apply(replace_chars)
        
        else:
            sql = self._update_table_names(series=series)
            
            # Execute query
            self.__run_query(sql, msg='Updated names in "{}".'.format(self.table))
            
            return self
                    
    # Add new columns to database
    def add_columns_from_data(self, data):
        """
        Adds columns from a pandas Dataframe that are not already present in 
        the table. 

        Params
        -------
        data : pandas Dataframe
            Dataframe with columns to be added to table
        """
        
        # Get names of current columns in PostgreSQL table
        current_names = self.get_names().tolist()

        # Get names in updated dataframe not in current table
        updated_names = data.columns.tolist()
        new_names = list(set(updated_names) - set(current_names))

        # Check names list is not empty
        if not new_names:
            print("Table columns are already up to date.")
            return

        # Format strings for query
        alter_table_sql = "ALTER TABLE {db_table}\n"
        add_column_sql = "\tADD COLUMN {column} TEXT,\n"

        # Create a list and append ADD column statements
        sql_query = [alter_table_sql.format(db_table=self.table)]
        for name in new_names:
            sql_query.append(add_column_sql.format(column=name))

        # Join into one string
        sql = ''.join(sql_query)[:-2] + ";"

        # Execute query
        self.__run_query(sql, msg='Added new columns to "{name}":\n{cols}'.format(name=self.table, cols=new_names))
        
        return self
    
    # Check whether dataframe columns match database table columns before running queries
    def _match_column_order(self, data):
        """
        Rearranges columns in dataframe to match table in order
        to avoid errors when updating values.
        """
        
        # Get columns from database as list
        db_columns = self.get_names().tolist()

        # Select columns from dataframe as list
        data_columns = data.columns.tolist()
        
        if set(data_columns) == set(db_columns):
            if data_columns != db_columns:
                print('Dataframe columns do not match table "{}".'.format(self.table))
                #data = data[db_columns]
                return True
            else:
                print('Dataframe columns already match table "{}".'.format(self.table))
                return True
        else:
            if len(data_columns) > len(db_columns):
                print('Dataframe has columns not in table "{}":'.format(self.table))
                print(list(set(data_columns) - set(db_columns)))
                return False
            else:
                print('Dataframe missing columns that are in table "{}":'.format(self.table))
                print(list(set(db_columns) - set(data_columns)))
                return False
        
    def _copy_from_dataframe(self, data, id_col, columns=None):
        """
        Copies rows from dataframe into a temporary table. Automatically 
        matches the order of columns between the table and the dataframe.
        Internal to update_values.
        """
        
        tmp_table = "tmp_" + self.table

        # Tests whether dataframe columns and table columns are same order
        match = self._match_column_order(data)

        # Match columns order between table and dataframe
        if match:
            # Get columns from table as list
            db_columns = self.get_names().tolist()
            
            # Select columns from dataframe as list
            data_columns = data.columns.tolist()

            # Rearrange to match
            data = data[db_columns]
            print('Rearranged dataframe columns to match "{}".'.format(self.table))

        try:
            con = self.__connect()
        except Exception as e:
            print("Connection Error:", e)

        columns = data.columns.tolist() if not columns else columns

        dataStream = StringIO()
        data.to_csv(dataStream, index=False, header=True, sep=',')
        dataStream.seek(0)
        
        sql = """
        COPY {tmp_table} FROM STDIN WITH (FORMAT CSV, HEADER TRUE);
        """.format(tmp_table=tmp_table)
        
        try:
            cur = con.cursor()
            cur.copy_expert(sql, dataStream)
            con.commit()
            cur.close()
            print('Copy successful on table "{}".'.format(self.table))
        except Exception as e:
            con.rollback()
            print("Error:", e)
        finally:
            if con is not None:
                con.close()
                
        return self          
        
    def _update_from_temp(self, id_col, columns=None):
        """
        Updates table from temporary table. Internal to update_values.
        """
        
        temp_table = "tmp_" + self.table
        columns = self.get_names().tolist() if not columns else columns
        sql_update = 'UPDATE {table}\n'.format(table=self.table)
        sql_set = ["SET "]
        
        for name in columns:
            line = "{name} = {tmp_name},\n\t".format(name=name, tmp_name=temp_table + '.' + name)
            sql_set.append(line)

        sql_set = ''.join(sql_set)
        sql_set = sql_set[:-3] + "\n"

        sql_from = "FROM {temp_table}\nWHERE {this_table}.{id_col} = {temp_table}.{id_col};\n\n" \
                            .format(temp_table=temp_table, this_table=self.table, id_col=id_col)
        sql_drop = 'DROP TABLE {};\n'.format(temp_table)
                
        sql = sql_update + sql_set + sql_from + sql_drop

        # Execute query
        self.__run_query(sql, msg='Updated values in "{}".'.format(self.table))
        
        return self
                  
    # Builds a query to update postgres from a csv file
    def update_values(self, data, id_col, types_dict, columns=None, sep=','):
        """
        Updates values in dataframe into table. If columns are in the
        dataframe but not in the table, will automatically add those 
        columns and update their types.
        """

        # Automatically updates table with new columns in dataframe
        if data.columns.tolist() != columns:
                self.add_columns_from_data(data)
                self.update_types(types_dict=types_dict, columns=columns)        
        
        columns = self.get_names().tolist() if not columns else [id_col] + columns
        
        column_params = {"id_col":id_col, "columns":columns}

        self.__create_temp_table(types_dict=types_dict, **column_params) \
                        ._copy_from_dataframe(data=data, **column_params) \
                        ._update_from_temp(**column_params)
        
        return

    # Updates column types in PostgreSQL database
    def update_types(self, types_dict, columns=None):
        """
        Updates types using types_dict, eg., 

        { "column name": "VARCHAR(100)", ... }

        """
        
        # Subset types based on columns input
        types_dict, columns = self.__subset_types_dict(types_dict, columns)
        
        # Define SQL update queries
        sql_alter_table = "ALTER TABLE public.{}\n\t".format(self.table)

        # Update types
        sql_update_types = []
        
        for column, col_type in types_dict.items():
            if "DATE" in col_type.upper():
                sql_string = "ALTER {column} TYPE {col_type} USING {column}::" + "{col_type},\n\t"
            elif "INT" in col_type.upper() or "NUM" in col_type.upper():
                sql_string = "ALTER {column} TYPE {col_type} USING {column}::text::numeric::{col_type},\n\t"
            elif "NUM" in col_type.upper():
                sql_string = "ALTER {column} TYPE {col_type} USING {column}::text::numeric::{col_type},\n\t"
            else:
                sql_string = "ALTER {column} TYPE {col_type},\n\t"

            sql_alter_column = sql_string.format(column=column, col_type=col_type)
            sql_update_types.append(sql_alter_column)

        # Join strings to create full sql query
        sql_update_types = sql_alter_table + ''.join(sql_update_types)

        # Replace very last character with ";"
        sql = sql_update_types[:-3] + ";"

        self.__run_query(sql, msg='Updated types in "{}".'.format(self.table))
            
        return 
