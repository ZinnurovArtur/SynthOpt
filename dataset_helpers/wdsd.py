from synthopt.process.db_adapter import TrinoDBAdapter


SELECT_AR_RESIDENCY_GPREG_20250602 = "SELECT * FROM iceberg.wdsd.per_residence_gpreg_20250602"
SELECT_SINGLE_CLEAN_AR_PERS = "SELECT * FROM iceberg.wdsd.wdsd_single_clean_ar_pers"
SELECT_CLEAN_GEOCHAR_LSOA2001 = "SELECT * FROM iceberg.wdsd.wdsd_single_clean_geo_char_lsoa2001"
SELECT_CLEAN_GEOCHAR_LSOA2011 = "SELECT * FROM iceberg.wdsd.wdsd_single_clean_geo_char_lsoa2011"
SELECT_SINGLE_CLEAN_GEO_RALF = "SELECT * FROM iceberg.wdsd.wdsd_single_clean_geo_ralf"
SELECT_SINGLE_CLEAN_GEO_RALF_LSOA2001 = "SELECT * FROM iceberg.wdsd.wdsd_single_clean_geo_ralf_lsoa2001"
SELECT_SINGLE_CLEAN_GEO_RALF_LSOA2011 = "SELECT * FROM iceberg.wdsd.wdsd_single_clean_geo_ralf_lsoa2011"
SELECT_SINGLE_CLEAN_GEO_WALES = "SELECT * FROM iceberg.wdsd.wdsd_single_clean_geo_wales"


def get_tables(adapter: TrinoDBAdapter, limit: int = 10000, offset: int = 0,selectQuery: str = ""):
    cursor = adapter.get_cursor()
    query = f'''
        {selectQuery}
        OFFSET {offset} LIMIT {limit}
    '''
    cursor.execute(query)
    return cursor.fetchall()

