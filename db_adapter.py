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

    def save_synthetic_table(self, table_name, data, schema='iceberg.arthur', append=False):
        """
        Save synthetic data (dict of lists or DataFrame) to a new table in Trino/Iceberg.
        Args:
            table_name (str): Name of the table to create (without schema prefix).
            data (dict of lists or DataFrame): Synthetic data, keys are column names, values are lists of values.
            schema (str): Schema to use (default 'iceberg.arthur').
        """
        import math
        import datetime
        import pandas as pd

        # Convert DataFrame to dict of lists if needed
        if isinstance(data, pd.DataFrame):
            data = {col: data[col].tolist() for col in data.columns}

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
        cursor = self.get_cursor()
        if not append:
            print(f"Creating table with: {create_sql}")
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
        # Insert data in batches for efficiency
        batch_size = 1000
        for batch_start in range(0, n_rows, batch_size):
            batch_end = min(batch_start + batch_size, n_rows)
            batch_rows = []
            for i in range(batch_start, batch_end):
                row = [data[col][i] for col in columns]
                sql_values = '(' + ', '.join([py_to_sql_literal(v) for v in row]) + ')'
                batch_rows.append(sql_values)
            values_clause = ', '.join(batch_rows)
            insert_sql = f'INSERT INTO {full_table_name} ({col_list}) VALUES {values_clause}'
            try:
                cursor.execute(insert_sql)
                print(f"Inserted rows {batch_start+1}-{batch_end} of {n_rows}")
            except Exception as e:
                print(f"Error inserting batch {batch_start+1}-{batch_end}: {e}")
        cursor.close()
        print(f"Done writing {n_rows} rows to {full_table_name}.")


if __name__ == "__main__":
    adapter = TrinoDBAdapter(username="username",host="trino.feasibility.sail.pk.serp.ac.uk")
    df = adapter.query("SELECT ss.* FROM iceberg.pedw.superspell_20231127 ss")
    print(df.head())