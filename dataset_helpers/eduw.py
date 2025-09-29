from db_adapter import TrinoDBAdapter


def get_table_pupil_alf(adapter: TrinoDBAdapter, limit: int = 10000, offset: int = 0):
    # Get data from the pupil_alf table with pagination and offset
    cursor = adapter.get_cursor()
    query = f'''
        SELECT * FROM (
            SELECT *, row_number() OVER () as rn
            FROM iceberg.eduw.pupil_alf
        ) t
        WHERE rn > {offset} AND rn <= {offset} + {limit}
    '''
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows


if __name__ == "__main__":
    #Provide your Trino username
    adapter = TrinoDBAdapter(username="username",host="trino.feasibility.sail.pk.serp.ac.uk")
    rows = get_table_pupil_alf(adapter, limit=10000, offset=0)
    adapter.engine.close()