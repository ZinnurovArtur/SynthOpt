import pandas as pd
import warnings
from sqlalchemy import create_engine, text
from trino.auth import OAuth2Authentication
import trino

class TrinoDBAdapter:
    def __init__(self, username, host, port=443):
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
        cursor = self.engine.cursor()
        cursor.execute(sql)
        return pd.DataFrame(cursor.fetchall())
    
    def get_cursor(self):
        cursor = self.engine.cursor()
        return cursor

    def save_synthetic_table(self, table_name, data, schema='iceberg.arthur'):
        """
        Save synthetic data (dict of lists) to a new table in Trino/Iceberg.
        Args:
            table_name (str): Name of the table to create (without schema prefix).
            data (dict of lists): Synthetic data, keys are column names, values are lists of values.
            schema (str): Schema to use (default 'iceberg.arthur').
        """
        import math
        import datetime

        # Infer column types from the first non-None value in each column
        type_map = {
            int: 'BIGINT',
            float: 'DOUBLE',
            str: 'VARCHAR',
            bool: 'BOOLEAN',
            datetime.datetime: 'TIMESTAMP',
        }
        columns = list(data.keys())
        n_rows = len(next(iter(data.values()))) if data else 0
        col_types = {}
        for col in columns:
            col_type = 'VARCHAR'  # default
            for v in data[col]:
                if v is not None and not (isinstance(v, float) and math.isnan(v)):
                    py_type = type(v)
                    if py_type in type_map:
                        col_type = type_map[py_type]
                    elif py_type == str:
                        col_type = 'VARCHAR'
                    elif py_type == int:
                        col_type = 'BIGINT'
                    elif py_type == float:
                        col_type = 'DOUBLE'
                    elif py_type == bool:
                        col_type = 'BOOLEAN'
                    elif isinstance(v, datetime.datetime):
                        col_type = 'TIMESTAMP'
                    break
            col_types[col] = col_type

        # Build CREATE TABLE statement
        col_defs = ', '.join([f'"{col}" {col_types[col]}' for col in columns])
        full_table_name = f'{schema}.{table_name}'
        create_sql = f'CREATE TABLE IF NOT EXISTS {full_table_name} ({col_defs})'
        print(f"Creating table with: {create_sql}")
        cursor = self.get_cursor()
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {full_table_name}")
            cursor.execute(create_sql)
        except Exception as e:
            print(f"Error creating table: {e}")
            cursor.close()
            return

        # Insert data row by row
        def py_to_sql_literal(val):
            if val is None or (isinstance(val, float) and math.isnan(val)):
                return 'NULL'
            elif isinstance(val, str):
                safe_val = val.replace("'", "''")
                return f"'{safe_val}'"
            elif isinstance(val, bool):
                return 'TRUE' if val else 'FALSE'
            elif isinstance(val, (int, float)):
                return str(val)
            elif isinstance(val, datetime.datetime):
                return f"TIMESTAMP '{val.isoformat(sep=' ')}'"
            else:
                safe_val = str(val).replace("'", "''")
                return f"'{safe_val}'"

        col_list = ', '.join([f'"{col}"' for col in columns])
        print(f"Inserting {n_rows} rows into {full_table_name}...")
        for i in range(n_rows):
            row = [data[col][i] for col in columns]
            sql_values = ', '.join([py_to_sql_literal(v) for v in row])
            insert_sql = f'INSERT INTO {full_table_name} ({col_list}) VALUES ({sql_values})'
            try:
                cursor.execute(insert_sql)
                if (i+1) % 100 == 0 or i == n_rows-1:
                    print(f"Inserted {i+1}/{n_rows} rows...")
            except Exception as e:
                print(f"Error inserting row {i+1}: {e}")
        cursor.close()
        print(f"Done writing {n_rows} rows to {full_table_name}.")


if __name__ == "__main__":
    adapter = TrinoDBAdapter(username="username",host="trino.feasibility.sail.pk.serp.ac.uk")
    df = adapter.query("SELECT ss.* FROM iceberg.pedw.superspell_20231127 ss")
    print(df.head())