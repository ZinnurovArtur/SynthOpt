import os

from synthopt.process.structural_metadata import process_structural_metadata, process_structural_metadata_sql
from synthopt.generate.structural_synthetic_data import generate_structural_synthetic_data, generate_structural_synthetic_data_from_sql
from pedw import get_table_admisions
from db_adapter import TrinoDBAdapter
import pandas as pd

# Set date_formats to None as in new_test.py
# You can adjust this if you want to specify formats

date_formats = None

# Progress tracking file
PROGRESS_FILE = 'progress.txt'
CHUNK_SIZE = 1000

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


def main():
    offset = get_last_offset()
    first_chunk = (offset == 0)
    while True:
        rows = get_table_admisions(adapter, limit=CHUNK_SIZE, offset=offset)
        if not rows:
            print("All records processed.")
            break
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
        DATA = pd.DataFrame(rows, columns=columns, dtype=str)
        STRUCT_METADATA = process_structural_metadata(DATA, datetime_formats=date_formats)
        STRUCT_SYNTHETIC_DATA = generate_structural_synthetic_data(STRUCT_METADATA, num_records=len(rows))
        # Save synthetic data, append if not first chunk
        adapter.save_synthetic_table('pedw_admissions_20231127_structural_synthetic', STRUCT_SYNTHETIC_DATA, append=not first_chunk)
        offset += len(rows)
        save_offset(offset)
        print(f"Processed and saved up to row {offset}")
        first_chunk = False

if __name__ == "__main__":
    main()