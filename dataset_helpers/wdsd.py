from synthopt.process.db_adapter import TrinoDBAdapter



WDSD_TABLES = {
    'per_residence_gpreg_20250602': 'iceberg.wdsd.per_residence_gpreg_20250602',
    'wdsd_single_clean_ar_pers': 'iceberg.wdsd.wdsd_single_clean_ar_pers',
    'wdsd_single_clean_geo_char_lsoa2001': 'iceberg.wdsd.wdsd_single_clean_geo_char_lsoa2001',
    'wdsd_single_clean_geo_char_lsoa2011': 'iceberg.wdsd.wdsd_single_clean_geo_char_lsoa2011',
    'wdsd_single_clean_geo_ralf': 'iceberg.wdsd.wdsd_single_clean_geo_ralf',
    'wdsd_single_clean_geo_ralf_lsoa2001': 'iceberg.wdsd.wdsd_single_clean_geo_ralf_lsoa2001',
    'wdsd_single_clean_geo_ralf_lsoa2011': 'iceberg.wdsd.wdsd_single_clean_geo_ralf_lsoa2011',
    'wdsd_single_clean_geo_wales': 'iceberg.wdsd.wdsd_single_clean_geo_wales'
}

def get_tables(adapter: TrinoDBAdapter, limit: int = 10000, offset: int = 0,selectQuery: str = ""):
    cursor = adapter.get_cursor()
    query = f'''
        "SELECT * FROM {selectQuery}
        OFFSET {offset} LIMIT {limit}
    '''
    cursor.execute(query)
    return cursor.fetchall()

