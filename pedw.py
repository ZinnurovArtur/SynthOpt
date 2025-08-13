from db_adapter import TrinoDBAdapter

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


if __name__ == "__main__":
    show_tables_and_select_all(TrinoDBAdapter(username="zinnurar",host="trino.feasibility.sail.pk.serp.ac.uk"))
