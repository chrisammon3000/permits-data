import os
import sys
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
import pandas as pd
import psycopg2

sys.path[0] = str(Path(__file__).resolve().parents[2]) # Set path for custom modules

# if modulename not in sys.modules: print...
load_dotenv(find_dotenv());

class Database():    
    def __init__(self, user="postgres", password="postgres",
                 dbname=None, host=None, port=5432):

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
    

    def _create_temp_table(self, types_dict, id_col, columns=None):
        
        # Append id_col to selected columns
        columns = None if not columns else [id_col] + columns
        
        # CREATE TABLE query
        tmp_table = "tmp_" + self.table
        
        # Subsets types_dict by columns argument and formats into string if no columns are specified
        types_dict = types_dict if not columns else {key:value for key, value in types_dict.items() if key in set(columns)}
        names = ',\n\t'.join(['{key} {val}'.format(key=key, val=val) for key, val in types_dict.items()])
        
        # Build queries
        sql = 'DROP TABLE IF EXISTS {tmp_table};\n\n'.format(tmp_table=tmp_table)
        sql = sql + 'CREATE TABLE {tmp_table} (\n\t{names}\n);\n\n' \
                                .format(tmp_table=tmp_table, names=names)
        
        # Execute query
        self._run_query(sql)
        
        return self
        