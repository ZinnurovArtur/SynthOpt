import os

from synthopt.process.structural_metadata import process_structural_metadata, process_structural_metadata_sql
from synthopt.generate.structural_synthetic_data import generate_structural_synthetic_data, generate_structural_synthetic_data_from_sql
from pedw import get_table_admisions
from db_adapter import TrinoDBAdapter
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Set date_formats to None as in new_test.py
# You can adjust this if you want to specify formats

date_formats = ["%Y-%m-%d"]

# Progress tracking file
PROGRESS_FILE = 'progress.txt'
CHUNK_SIZE = 5000

# Add a lock for thread-safe progress file updates
progress_lock = threading.Lock()

# Instantiate the adapter (update username/host as needed)
adapter = TrinoDBAdapter(username="zinnurar", host="trino.feasibility.sail.pk.serp.ac.uk")

def get_last_offset():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            try:
                return int(f.read().strip())
            except Exception:
                return 0
    return 0

def save_offset(offset):
    with open(PROGRESS_FILE, 'w') as f:
        f.write(str(offset))


def process_and_save_chunk(offset, chunk_size, first_chunk):
    rows = get_table_admisions(adapter, limit=chunk_size, offset=offset)
    if not rows:
        return 0
    # Get column names from the cursor description
    cursor = adapter.get_cursor()
    cursor.execute(f'''
        SELECT * FROM (
            SELECT *, row_number() OVER () as rn
            FROM iceberg.pedw.pedw_admissions_20231127
        ) t
        WHERE rn = {offset + 1}
    ''')
    columns = [desc[0] for desc in cursor.description]
    cursor.close()
    # Convert to DataFrame
    DATA = pd.DataFrame(rows, columns=columns)
    STRUCT_METADATA = process_structural_metadata(DATA, datetime_formats=date_formats)
    STRUCT_SYNTHETIC_DATA = generate_structural_synthetic_data(STRUCT_METADATA, num_records=len(rows))
    # Save synthetic data, append if not first chunk
    adapter.save_synthetic_table('pedw_admissions_20231127_structural_synthetic', STRUCT_SYNTHETIC_DATA, append=not first_chunk)
    with progress_lock:
        save_offset(offset + len(rows))
    print(f"Processed and saved up to row {offset + len(rows)}")
    return len(rows)

def threading_code():
    offset = get_last_offset()
    first_chunk = (offset == 0)
    max_workers = 5  # Tune this based on your system/network
    # Estimate total rows if you want to stop at a certain point, or set a very high number
    TOTAL_ROWS = 100  # Or set to None for unknown
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        chunk_offsets = list(range(offset, TOTAL_ROWS, CHUNK_SIZE))
        for i, chunk_offset in enumerate(chunk_offsets):
            is_first = first_chunk and (i == 0)
            futures.append(executor.submit(process_and_save_chunk, chunk_offset, CHUNK_SIZE, is_first))
        for future in as_completed(futures):
            rows_processed = future.result()


def testing_code():
    cursor = adapter.get_cursor()
    rows = get_table_admisions(adapter, limit=100, offset=0)
   
    cursor.execute(f'''
        SELECT * FROM (
            SELECT *, row_number() OVER () as rn
            FROM iceberg.pedw.pedw_admissions_20231127
        ) t
        WHERE rn = {0 + 1}
    ''')
    columns = [desc[0] for desc in cursor.description]
    cursor.close()

    DATA = pd.DataFrame.from_records(rows, columns=columns)
    STRUCT_METADATA = process_structural_metadata(DATA, datetime_formats=date_formats)
    print(STRUCT_METADATA)
    STRUCT_SYNTHETIC_DATA = generate_structural_synthetic_data(STRUCT_METADATA, num_records=len(rows))
    print(STRUCT_SYNTHETIC_DATA)
    #adapter.save_synthetic_table('pedw_admissions_20231127_structural_synthetic', STRUCT_SYNTHETIC_DATA, append=False)


if __name__ == "__main__":
    testing_code()