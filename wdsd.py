def get_table_admisions(adapter: TrinoDBAdapter, limit: int = 10000, offset: int = 0):
    cursor = adapter.get_cursor()
    query = f'''
        SELECT * FROM iceberg.pedw.pedw_admissions_20231127
        ORDER BY alf_e
        OFFSET {offset} LIMIT {limit}
    '''
    cursor.execute(query)
    return cursor.fetchall()

def get_ar_residency_gpreg_table(adapter: TrinoDBAdapter, limit: int = 10000, offset: int = 0):
    cursor = adapter.get_cursor()
    query = f'''
    SELECT * FROM iceberg.wdsd.ar_residency_gpreg_20250602
    ORDER BY alf_e
    OFFSET {offset} LIMIT {limit}
    '''
    cursor.execute(query)
    return cursor.fetchall()

