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
    # Use ROW_NUMBER() for pagination since Trino does not support OFFSET
    query = f'''
        SELECT * FROM (
            SELECT *, row_number() OVER () as rn
            FROM iceberg.pedw.pedw_admissions_20231127
        ) t
        WHERE rn > {offset} AND rn <= {offset} + {limit}
    '''
    cursor.execute(query)
    return cursor.fetchall()


if __name__ == "__main__":
    show_tables_and_select_all(TrinoDBAdapter(username="zinnurar",host="trino.feasibility.sail.pk.serp.ac.uk"))
