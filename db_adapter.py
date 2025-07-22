import pandas as pd
import warnings
from sqlalchemy import create_engine, text
from trino.auth import OAuth2Authentication

class TrinoDBAdapter:
    def __init__(self, username, host, port=443, verify=False):
        self.engine = create_engine(
            f"trino://{username}@{host}:{port}",
            connect_args={
                "auth": OAuth2Authentication(),
                "http_scheme": "https",
                "verify": verify
            }
        )


    def query(self, sql):
        with self.engine.connect() as connection:
            return pd.read_sql(sql, connection)
    
    def get_cursor(self):
        conn = self.engine.connect()
        cursor = conn.connection.cursor()
        return cursor, conn

if __name__ == "__main__":
    adapter = TrinoDBAdapter(username="username",host="trino.feasibility.sail.pk.serp.ac.uk")
    df = adapter.query("SELECT ss.* FROM iceberg.pedw.superspell_20231127 ss")
    print(df.head())