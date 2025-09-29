import pandas as pd
import warnings
from sqlalchemy import create_engine, text
from trino.auth import OAuth2Authentication
import trino
import sys
import math
import datetime
import keyring
import math, datetime as dt

class TrinoDBAdapter:
    def __init__(self, username, host, port=443):
        # Constuctor to initialize the TrinoDBAdapter with connection parameters.

        self.engine = trino.dbapi.connect(
            host=host,
            port=port,
            user=username,
            auth=OAuth2Authentication(),
            http_scheme="https",
            catalog="iceberg",
            verify=False
            
        )
  
    def query(self, sql):
        #Testing query function to execute a SQL query and return the results as a pandas DataFrame.
        cursor = self.engine.cursor()
        cursor.execute(sql)
        return pd.DataFrame(cursor.fetchall())
    
    def get_cursor(self):
        # Function to get a cursor from the Trino connection.
        cursor = self.engine.cursor()
        return cursor

    def set_writer_settings(self, cursor):
        # This function sets session properties for writing data to Iceberg tables. 
        # This settings is neccesary for not bloating the S3 bucket 
        cursor = self.get_cursor()
        cursor.execute("SET SESSION scale_writers = true")
        cursor.execute("SET SESSION iceberg.target_max_file_size = '1GB'")
        cursor.close()

    def ensure_table(self, full_table_name: str, columns_sql: str = None):
        cur = self.get_cursor()
        # If you already have the table, you can skip CREATE. Otherwise define once.
        if columns_sql:
            cur.execute(f"CREATE TABLE IF NOT EXISTS {full_table_name} ({columns_sql})")
        cur.close()

    def insert_rows_batch(self, full_table_name: str, data: dict, columns: list, cursor,
                        max_sql_chars: int = 900_000, max_tuples_per_insert: int = 10_000):
        
        # Insert rows batches into the table, with the conversion from dataframe

        def lit(v):
            if v is None or (isinstance(v, float) and math.isnan(v)): return 'NULL'
            if isinstance(v, str):   return "'" + v.replace("'", "''") + "'"
            if isinstance(v, bool):  return 'TRUE' if v else 'FALSE'
            if isinstance(v, (int, float)): return str(v)
            if isinstance(v, dt.datetime):  return f"TIMESTAMP '{v.isoformat(sep=' ')}'"
            return "'" + str(v).replace("'", "''") + "'"

        n = len(next(iter(data.values()))) if data else 0
        cols = ', '.join(f'"{c}"' for c in columns)
        prefix = f"INSERT INTO {full_table_name} ({cols}) VALUES "
        prefix_len = len(prefix)

        batch_values = []
        current_len = prefix_len


        def flush():
            # Flush the current batch of values to the database.
            nonlocal batch_values, current_len
            if not batch_values:
                return 0
            sql = prefix + ", ".join(batch_values)
            cursor.execute(sql)
            cnt = len(batch_values)
            batch_values = []
            current_len = prefix_len
            return cnt

        inserted = 0
        for i in range(n):
            row = [data[c][i] for c in columns]
            tuple_sql = "(" + ", ".join(lit(x) for x in row) + ")"
            projected_len = current_len + (2 if batch_values else 0) + len(tuple_sql)
            projected_cnt = len(batch_values) + 1

            if projected_len > max_sql_chars or projected_cnt > max_tuples_per_insert:
                inserted += flush()
            batch_values.append(tuple_sql)
            current_len += (2 if len(batch_values) > 1 else 0) + len(tuple_sql)

        inserted += flush()
        return inserted


if __name__ == "__main__":
    adapter = TrinoDBAdapter(username="username",host="trino.feasibility.sail.pk.serp.ac.uk")
    df = adapter.query("SELECT ss.* FROM iceberg.pedw.superspell_20231127 ss")
    print(df.head())