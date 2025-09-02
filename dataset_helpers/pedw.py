from synthopt.process.db_adapter import TrinoDBAdapter
from functools import lru_cache

# Cache for column names to avoid repeated database calls
_column_names_cache = None

def show_tables_and_select_all(adapter: TrinoDBAdapter):

    cursor = adapter.get_cursor()
    try:
        # List all tables in the schema
        cursor.execute("SHOW TABLES FROM iceberg.pedw")
        print("Tables in iceberg.pedw:")
        for row in cursor.fetchall():
            print(row)

        # Select all rows from the table
        print("\nAll rows from iceberg.pedw.superspell_20231127:")
        cursor.execute("SELECT * FROM iceberg.pedw.superspell_20231127")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    finally:
        cursor.close()
        adapter.engine.close()

def get_table_admisions(adapter: TrinoDBAdapter, limit: int = 10000, offset: int = 0):
    cursor = adapter.get_cursor()
    query = f'''
        SELECT * FROM iceberg.pedw.pedw_admissions_20231127
        ORDER BY alf_e
        OFFSET {offset} LIMIT {limit}
    '''
    cursor.execute(query)
    return cursor.fetchall()


@lru_cache(maxsize=1)
def get_admissions_row_count(adapter: TrinoDBAdapter):
    """Get the actual number of rows in pedw_admissions table (cached)"""
    cursor = adapter.get_cursor()
    cursor.execute("SELECT COUNT(*) FROM iceberg.pedw.pedw_admissions_20231127")
    count = cursor.fetchone()[0]
    cursor.close()
    print(f"Found {count} total rows in pedw_admissions table")
    return count

@lru_cache(maxsize=1)
def get_column_names(adapter: TrinoDBAdapter):
    """Get column names once and cache them"""
    global _column_names_cache
    if _column_names_cache is None:
        cursor = adapter.get_cursor()
        cursor.execute("SELECT * FROM iceberg.pedw.pedw_admissions_20231127 LIMIT 1")
        _column_names_cache = [desc[0] for desc in cursor.description]
        cursor.close()
    return _column_names_cache

if __name__ == "__main__":
    show_tables_and_select_all(TrinoDBAdapter(username="zinnurar",host="trino.feasibility.sail.pk.serp.ac.uk"))
