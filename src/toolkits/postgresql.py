import os
import sys
from pathlib import Path
sys.path[0] = str(Path(__file__).resolve().parents[2]) # Set path for custom modules
from dotenv import load_dotenv, find_dotenv
import numpy as np
import pandas as pd
import psycopg2
import csv
import warnings
from io import StringIO
from src.pipeline.dictionaries import types_dict, replace_map

# if modulename not in sys.modules: print...
load_dotenv(find_dotenv());


#### Database class ####

class Database():
    
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
                
                
    def _run_query(self, sql):
        
        try:
            con = self._connect()
        except Exception as e:
            print("Error:", e)            
            
        try:
            cur = con.cursor()
            cur.execute(sql)
            con.commit()
            cur.close()
            print('Query successful on database "{}".'.format(self.dbname))
        except Exception as e:
            con.rollback()
            print("Error:", e)
        finally:
            if con is not None:
                con.close()
        
        return

    
    def create_table(self, table_name, types_dict, id_col, columns=None):
        
        # Append id_col to selected columns
        columns = None if not columns else set([id_col] + columns)
        
        # Subsets types_dict by columns argument and formats into string if no columns are specified
        types_dict = types_dict if not columns else {key:value for key, value in types_dict.items() if key in set(columns)}
        names = ',\n\t'.join(['{key} {val}'.format(key=key, val=val) for key, val in types_dict.items()])
        
        # Build queries
        sql = 'CREATE TABLE {table_name} (\n\t{names}\n);\n\n' \
                            .format(table_name=table_name, names=names) # + sql
        
        # Execute query
        self._run_query(sql)
        
        return self
    
    
    def drop_table(self, table_name):
        
        # Build queries
        sql = 'DROP TABLE IF EXISTS {table_name};\n\n'.format(table_name=table_name)
        
        # Execute query
        self._run_query(sql)
        
        return self
    
    
    def _subset_types_dict(self, types_dict, columns):

        columns = self.get_names().tolist() if not columns else columns
        types_dict = {key:value for key, value in types_dict.items() if key in set(columns)}
        
        return types_dict, columns
    

    def _create_temp_table(self, types_dict, id_col, columns=None):
        
        # Append id_col to selected columns
        columns = None if not columns else [id_col] + columns

        # CREATE TABLE query
        tmp_table = "tmp_" + self.table

        sql = """
        DROP TABLE IF EXISTS {tmp_table};
        CREATE TABLE {tmp_table} AS (SELECT * FROM {table}) WITH NO DATA;
        """.format(tmp_table=tmp_table, table=self.table)


        # Execute query
        self._run_query(sql)
        
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
        
        tables = []
        
        for result in results:
            tables.append(*result)
            
        return tables
        

#### Table class ####

class Table(Database):
    def __init__(self, name, user="postgres", password="postgres",
                 dbname=None, host="localhost", port=5432):
        
        super().__init__(user, password, dbname, host, port)
        
        self.table = name
        
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
    def __run_query(self, sql):
        return super(Table, self)._run_query(sql)
    
    def __subset_types_dict(self, types_dict, columns):
        return super(Table, self)._subset_types_dict(types_dict, columns)

    def __create_temp_table(self, types_dict, id_col, columns):
        return super(Table, self)._create_temp_table(types_dict, id_col, columns)
    
    
    # Fetch data from sql query
    def fetch_data(self, sql=None, coerce_float=False, parse_dates=None):
        
        sql = sql or "SELECT * FROM {};".format(self.table)
        
        con = self.__connect()
        
        # Fetch fresh data
        data = pd.read_sql_query(sql=sql, con=con, coerce_float=coerce_float, parse_dates=parse_dates)
        
        # Recast integer columns to preserve original types
        update_dict = self.get_types(pandas_integers=True)
        update_dict = {k: v for k, v in update_dict.items() if v}
        data = data.astype(update_dict)
        
        # Replace None with np.nan
        data.fillna(np.nan, inplace=True)
        
        # Close db connection
        con.close()

        return data
    
    
    # Get names of column
    def get_names(self):
        
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
            sql_query = self._update_table_names(series=series)
            
            # Execute query
            self.__run_query(sql_query)
            
            return self
                    

    # Add new columns to database
    def add_columns_from_data(self, data):
        
        # Get names of current columns in PostgreSQL table
        current_names = self.get_names().tolist()

        # Get names of updated table not in current table
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
        self.__run_query(sql)
        
        return self
    
    
    # Check whether dataframe columns match database table columns before running queries
    def _match_column_order(self, data):
        
        # Get columns from database as list
        db_columns = self.get_names().tolist()

        # Select columns from dataframe as list
        data_columns = data.columns.tolist()
        
        if set(data_columns) == set(db_columns):
            if data_columns != db_columns:
                print('Rearranged dataframe columns to match table "{}".'.format(self.table))
                data = data[db_columns]
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
        
        tmp_table = "tmp_" + self.table

        match = self._match_column_order(data)

        if self._match_column_order(data):
            # Get columns from database as list
            db_columns = self.get_names().tolist()
            
            # Select columns from dataframe as list
            data_columns = data.columns.tolist()
            data = data[db_columns]

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
        self.__run_query(sql)
        
        return self
        
                
    # Builds a query to update postgres from a csv file
    def update_values(self, data, id_col, types_dict, columns=None, sep=','):

        if data.columns.tolist() != columns:
                self.add_columns_from_data(data)
                self.update_types(types_dict=types_dict, columns=columns)        
        
        columns = self.get_names().tolist() if not columns else [id_col] + columns
        
        column_params = {"id_col":id_col, "columns":columns}

        self.__create_temp_table(types_dict=types_dict, **column_params) \
                        ._copy_from_dataframe(data=data, **column_params) \
                        ._update_from_temp(**column_params)
        
    
    # Updates column types in PostgreSQL database
    def update_types(self, types_dict, columns=None):
        
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

        self.__run_query(sql)
            
        return 


#### Data class ####

# equal to table.fetch()
# convert functions into methods, create_full_address, geocode etc
# def __init__(self, data):
#     self.fetch_data(data)

# # Inherit from Table
# def fetch_data(self, data):
#     self.data = pd.read_csv(data)