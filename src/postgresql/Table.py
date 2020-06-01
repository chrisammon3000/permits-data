import os
import sys
from pathlib import Path
import warnings
from io import StringIO
from dotenv import load_dotenv, find_dotenv
import pandas as pd
import psycopg2
from src.pipeline.dictionaries import types_dict, replace_map

#sys.path[0] = str(Path(__file__).resolve().parents[2]) # Set path for custom modules

# if modulename not in sys.modules: print...
load_dotenv(find_dotenv());

class Table(Database):
    def __init__(self, user=None, password=None, dbname=None, host=None, port=None, table=None):
        super().__init__(user, password, dbname, host, port)
        
        self.table = table
        
        # Loaded from .env if not explicit
        self.user = user if user is not None else os.getenv("POSTGRES_USER")
        self.password = password if password is not None else os.getenv("POSTGRES_PASSWORD")
        self.dbname = dbname if dbname is not None else os.getenv("POSTGRES_DB")
        self.host = host if host is not None else os.getenv("DB_HOST")
        self.port = port if port is not None else os.getenv("DB_PORT")

    
    # Connect to database
    def __connect(self):
        return super(Table, self)._connect()
    
    # Check info on connection
    def __con(self):
        return super(Table, self)._con
    
    # Run query
    def __run_query(self, sql):
        return super(Table, self)._run_query(sql)
    
    # Run query
    def __create_temp_table(self, types_dict, id_col, columns):
        return super(Table, self)._create_temp_table(types_dict, id_col, columns)
    
    
    # Fetch data from sql query
    def fetch_data(self, sql, coerce_float=False, parse_dates=None):
        
        con = self.__connect()
        
        # Fetch fresh data
        data = pd.read_sql_query(sql=sql, con=con, coerce_float=coerce_float, parse_dates=parse_dates)

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
    def get_types(self, as_dataframe=False):
        
        # Specific query to retrieve table names
        sql = '''SELECT column_name, 
        CASE 
            WHEN domain_name is not null then domain_name
            WHEN data_type='character varying' THEN 'varchar('||character_maximum_length||')'
            WHEN data_type='character' THEN 'char('||character_maximum_length||')'
            WHEN data_type='numeric' THEN 'numeric'
            ELSE data_type
        END AS type
        FROM information_schema.columns WHERE table_name = 'permits_raw';
        '''
        
        # Run query and extract
        try:
            con = self.__connect()
            data = pd.read_sql_query(sql, con)
            con.close()
        except Exception as e:
            print("Error:", e)
        
        if as_dataframe:
            data['type'] = data['type'].str.upper()
            return data
        
        types_dict = dict(zip(data['column_name'], data['type'].str.upper()))
        
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
            warning.warn('No changes made. Set "update=False" to run query on database.')
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
        
        if self._match_column_order(data):
        
            try:
                con = self.__connect()
            except Exception as e:
                print("Error:", e)
            
            columns = data.columns.tolist() if not columns else columns
            temp_table = "tmp_" + self.table
            data_buffer = StringIO(data.to_csv(header=False, index=False, sep=','))

            try:
                cur = con.cursor()
                data_buffer.read()
                cur.copy_from(file=data_buffer, table=temp_table, columns=columns)
                data_buffer.close()
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
        
        columns = self.get_names().tolist() if not columns else [id_col] + columns
        params = {"id_col":id_col, "columns":columns}
        
        self.__create_temp_table(types_dict=types_dict, **params) \
                        ._copy_from_dataframe(data=data, **params) \
                        ._update_from_temp(**params)
        
                
####Update types