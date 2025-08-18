import os
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import random
from functools import lru_cache

from synthopt.process.structural_metadata import process_structural_metadata, process_structural_metadata_sql
from synthopt.generate.structural_synthetic_data import generate_structural_synthetic_data, generate_structural_synthetic_data_from_sql
from pedw import get_table_admisions
from db_adapter import TrinoDBAdapter
import pandas as pd

# Set date_formats
date_formats = ["%Y-%m-%d"]

# Progress tracking file
PROGRESS_FILE = 'pedw_progress.txt'
CHUNK_SIZE = 500_000         

TARGET_TABLE = "iceberg.arthur.pedw_admissions_20231127_structural_synthetic"


# Add a lock for thread-safe progress file updates
progress_lock = threading.Lock()

# Instantiate the adapter
adapter = TrinoDBAdapter(username="zinnurar", host="trino.feasibility.sail.pk.serp.ac.uk")

# Cache for column names to avoid repeated database calls
_column_names_cache = None

def ensure_target_table(metadata):
    """
    Create target table once if needed. If you already created it, you can skip columns_sql.
    We infer a schema from metadata here only if you really need CREATE TABLE.
    """
    # (Option A) If table exists already, just set writer props:

    adapter.ensure_table(TARGET_TABLE)

    # (Option B) If you need to CREATE, provide explicit columns:
    # columns_sql = """
    #   "col1" VARCHAR,
    #   "col2" BIGINT,
    #   ...
    # """
    # adapter.ensure_table(TARGET_TABLE, columns_sql=columns_sql)


def post_maintenance():
    cur = adapter.get_cursor()
    try:
        cur.execute(f"ALTER TABLE {TARGET_TABLE} EXECUTE optimize(file_size_threshold => '256MB')")
        cur.execute(f"ALTER TABLE {TARGET_TABLE} EXECUTE optimize_manifests")
        cur.execute(f"ALTER TABLE {TARGET_TABLE} EXECUTE expire_snapshots(retention_threshold => '3d')")
        cur.execute(f"ALTER TABLE {TARGET_TABLE} EXECUTE remove_orphan_files(retention_threshold => '3d')")
    finally:
        cur.close()


def generate_and_load_all(metadata):
 
    print("=== Generating New PEDW Synthetic Data ===")
    ensure_target_table(metadata)

    total_rows = get_admissions_row_count()
    start_offset = get_last_offset()
    print(f"Resuming at offset {start_offset} of {total_rows}")
    print(f"Chunk size = {CHUNK_SIZE}")

    cur = adapter.get_cursor()
    try:
        # Schema & writer settings
        cur.execute("USE iceberg.arthur")
        adapter.set_writer_settings(cur)

        committed_until = start_offset
        rows_since_maintenance = 0

        for offset in range(start_offset, total_rows, CHUNK_SIZE):
            remaining = total_rows - offset
            rows_to_generate = min(CHUNK_SIZE, remaining)
            print(f"Generating chunk offset={offset}, rows={rows_to_generate}")

            # generate synthetic data
            df = generate_structural_synthetic_data(metadata, num_records=rows_to_generate)
            cols = list(df.columns)
            data_dict = {c: df[c].tolist() for c in cols}

            # big INSERT(s), split if needed
            adapter.insert_rows_batch(
                full_table_name=TARGET_TABLE,
                data=data_dict,
                columns=cols,
                cursor=cur,
                max_tuples_per_insert=500_000,
                max_sql_chars=1_000_000,
            )

            committed_until = offset + rows_to_generate
            rows_since_maintenance += rows_to_generate

            # Periodic maintenance checkpoint
            if rows_since_maintenance >= 500_000:  # tune window
                print(f"Running maintenance at ~{committed_until} rows")
                post_maintenance()
                with progress_lock:
                    save_offset(committed_until)
                rows_since_maintenance = 0

        # final maintenance
        post_maintenance()
        with progress_lock:
            save_offset(committed_until)

    finally:
        cur.close()

    print("=== Load complete ===")

@lru_cache(maxsize=1)
def get_column_names():
    """Get column names once and cache them"""
    global _column_names_cache
    if _column_names_cache is None:
        cursor = adapter.get_cursor()
        cursor.execute("SELECT * FROM iceberg.pedw.pedw_admissions_20231127 LIMIT 1")
        _column_names_cache = [desc[0] for desc in cursor.description]
        cursor.close()
    return _column_names_cache

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

@lru_cache(maxsize=1)
def get_admissions_row_count():
    """Get the actual number of rows in pedw_admissions table (cached)"""
    cursor = adapter.get_cursor()
    cursor.execute("SELECT COUNT(*) FROM iceberg.pedw.pedw_admissions_20231127")
    count = cursor.fetchone()[0]
    cursor.close()
    print(f"Found {count} total rows in pedw_admissions table")
    return count



def collect_metadata_sample():
    """Collect metadata from a single sample of 100k rows"""
    print("=== Collecting Structural Metadata from 100k Sample ===")
        
    # Collect 100k rows for metadata
    SAMPLE_SIZE = 100000
    print(f"Collecting metadata from {SAMPLE_SIZE} rows")
    
    try:
        # Get sample data
        rows = get_table_admisions(adapter, limit=SAMPLE_SIZE, offset=0)
        if not rows:
            raise ValueError("No data retrieved from database")
        
        # Use cached column names
        columns = get_column_names()
        
        # Convert to DataFrame
        DATA = pd.DataFrame.from_records(rows, columns=columns)
        
        # Process structural metadata
        metadata = process_structural_metadata(DATA, datetime_formats=date_formats)
        
        print(f"Successfully collected metadata from {len(rows)} rows")
        
        return metadata
        
    except Exception as e:
        print(f"Error collecting metadata: {e}")
        raise

def generate_and_save_chunk(metadata, offset, chunk_size, first_chunk, total_rows):
    """Generate synthetic data for a chunk and save it using db_adapter"""
    # Calculate how many rows to generate for this chunk
    remaining_rows = total_rows - offset
    rows_to_generate = min(chunk_size, remaining_rows)
    
    if rows_to_generate <= 0:
        return 0
    
    try:
        print(f"Processing chunk: offset {offset}, rows {rows_to_generate}")
        
        # Generate synthetic data using metadata
        synthetic_data = generate_structural_synthetic_data(metadata, num_records=rows_to_generate)
        
        print(f"Generated synthetic data with {rows_to_generate} rows")
        
        # Save synthetic data using db_adapter,
        adapter.save_synthetic_table('pedw_admissions_20231127_structural_synthetic', synthetic_data, schema='iceberg.arthur')
        
        with progress_lock:
            save_offset(offset + rows_to_generate)
        
        print(f"Successfully processed {rows_to_generate} rows (offset {offset} -> {offset + rows_to_generate})")
        return rows_to_generate
        
    except Exception as e:
        print(f"Error processing chunk at offset {offset}: {e}")
        return 0

def generate_synthetic_data(metadata):
    """Generate completely new synthetic data using the collected metadata"""
    print("=== Generating New PEDW Synthetic Data ===")
    
    offset = get_last_offset()
    max_workers = 1  # Single worker to minimize conflicts and S3 metadata
    TOTAL_ROWS = get_admissions_row_count()
    
    print(f"Starting generation from offset {offset} for {TOTAL_ROWS} total rows")
    print(f"Using chunk size: {CHUNK_SIZE}, workers: {max_workers}")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        chunk_offsets = list(range(offset, TOTAL_ROWS, CHUNK_SIZE))
        
        for i, chunk_offset in enumerate(chunk_offsets):
            # First chunk should create the table (append=False), others should append (append=True)
            is_first_chunk = (i == 0)
            futures.append(executor.submit(generate_and_save_chunk, metadata, chunk_offset, CHUNK_SIZE, is_first_chunk, TOTAL_ROWS))
        
        for future in as_completed(futures):
            rows_processed = future.result()
            # Add small delay between chunk completions to reduce S3 metadata conflicts
            time.sleep(random.uniform(0.2, 0.8))

def main():
    """Main function for PEDW data processing with synthetic values"""
    print("=== Starting PEDW Data Processing with Synthetic Values ===")
    
    # Collect metadata from 100k sample from the REAL table
    metadata = collect_metadata_sample()

    # Generate new synthetic data using real table metadata
    generate_and_load_all(metadata)
    
    print("=== PEDW Data Processing Complete! ===")

if __name__ == "__main__":
    main()

