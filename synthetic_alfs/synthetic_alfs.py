import os
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import random
import pandas as pd

from synthopt.process.structural_metadata import process_structural_metadata
from synthopt.generate.structural_synthetic_data import generate_structural_synthetic_data
from db_adapter import TrinoDBAdapter

# Progress tracking file
PROGRESS_FILE = 'synthetic_alfs_progress.txt'
CHUNK_SIZE = 100_000         

USERNAME = "username"  # Replace with your Trino username
TARGET_TABLE = f"iceberg.{USERNAME}.synthetic_alfs" 
SOURCE_TABLE = f"iceberg.{USERNAME}.all_distinct_alfs"

# Add a lock for thread-safe progress file updates
progress_lock = threading.Lock()

# Instantiate the adapter
adapter = TrinoDBAdapter(username=USERNAME, host="trino.feasibility.sail.pk.serp.ac.uk")

def ensure_target_table():
    """
    Create target table for storing synthetic alf_e values
    """
    columns_sql = """
        "alf_e" BIGINT
    """
    adapter.ensure_table(TARGET_TABLE, columns_sql )

def post_maintenance():
    """Run maintenance operations on the target table"""
    cur = adapter.get_cursor()
    try:
        cur.execute(f"ALTER TABLE {TARGET_TABLE} EXECUTE optimize(file_size_threshold => '256MB')")
        cur.execute(f"ALTER TABLE {TARGET_TABLE} EXECUTE optimize_manifests")
        cur.execute(f"ALTER TABLE {TARGET_TABLE} EXECUTE expire_snapshots(retention_threshold => '2d')")
        cur.execute(f"ALTER TABLE {TARGET_TABLE} EXECUTE remove_orphan_files(retention_threshold => '2d')")
    finally:
        cur.close()

def collect_alfs_metadata_sample():
    """Collect metadata from a sample of ALF_E values"""
    print("=== Collecting Structural Metadata from ALF_E Sample ===")
        
    # Collect sample for metadata analysis
    SAMPLE_SIZE = 100000
    print(f"Collecting metadata from {SAMPLE_SIZE} ALF_E values")
    
    try:
        # Get sample data
        cursor = adapter.get_cursor()
        cursor.execute(f"SELECT alf_e FROM {SOURCE_TABLE} LIMIT {SAMPLE_SIZE}")
        rows = cursor.fetchall()
        cursor.close()
        
        if not rows:
            raise ValueError("No ALF_E data retrieved from database")
        
        # Convert to DataFrame
        DATA = pd.DataFrame(rows, columns=['alf_e'])
        
        # Process structural metadata
        metadata = process_structural_metadata(DATA)
        
        print(f"Successfully collected metadata from {len(rows)} ALF_E values")
        
        return metadata
        
    except Exception as e:
        print(f"Error collecting metadata: {e}")
        raise

def get_target_count():
    """
    Get the target number of synthetic ALF_E values to generate
    """
    cursor = adapter.get_cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE}")
        real_count = cursor.fetchone()[0]
        # Generate 2x the number of real ALF_E values
        target_count = real_count
        print(f"Found {real_count} real ALF_E values, will generate {target_count} synthetic values")
        return target_count
    finally:
        cursor.close()

def generate_and_load_synthetic_alfs(metadata, num_synthetic_alfs=None):
    """
    Generate synthetic ALF_E data using structural metadata and load to database
    """
    print("=== Generating Synthetic ALF_E Data ===")
    
    ensure_target_table()
    
    # Determine target count
    if num_synthetic_alfs is None:
        num_synthetic_alfs = get_target_count()
    
    print(f"Generating {num_synthetic_alfs} synthetic ALF_E values")
    print(f"Chunk size = {CHUNK_SIZE}")

    cur = adapter.get_cursor()
    try:
        # Schema & writer settings
        cur.execute(f"USE iceberg.{USERNAME}")
        adapter.set_writer_settings(cur)

        committed_until = 0
        rows_since_maintenance = 0

        for offset in range(0, num_synthetic_alfs, CHUNK_SIZE):
            remaining = num_synthetic_alfs - offset
            rows_to_generate = min(CHUNK_SIZE, remaining)
            print(f"Generating chunk offset={offset}, rows={rows_to_generate}")

            # Generate synthetic data using structural metadata
            df = generate_structural_synthetic_data(metadata, num_records=rows_to_generate)
            cols = list(df.columns)
            data_dict = {c: df[c].tolist() for c in cols}

            # Insert chunk
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
            if rows_since_maintenance >= 500_000:
                print(f"Running maintenance at ~{committed_until} rows")
                post_maintenance()
                with progress_lock:
                    save_offset(committed_until)
                rows_since_maintenance = 0

        # Final maintenance
        post_maintenance()
        with progress_lock:
            save_offset(committed_until)

    finally:
        cur.close()

    print("=== Load complete ===")

def get_last_offset():
    """Get the last saved offset from progress file"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            try:
                return int(f.read().strip())
            except Exception:
                return 0
    return 0

def save_offset(offset):
    """Save the current offset to progress file"""
    with open(PROGRESS_FILE, 'w') as f:
        f.write(str(offset))

def main(num_synthetic_alfs=None):
    """
    Main function to generate synthetic ALF_E values using structural metadata
    
    Args:
        num_synthetic_alfs (int, optional): Number of synthetic ALF_E values to generate.
                                          If None, will generate 2x the number of real ALF_E values.
    """
    print("=== Starting Synthetic ALF_E Generation using Structural Metadata ===")
    
    # Collect metadata from sample of real ALF_E values
    metadata = collect_alfs_metadata_sample()

    # Generate and load synthetic ALF_E data
    generate_and_load_synthetic_alfs(metadata, num_synthetic_alfs)
    
    print("=== Synthetic ALF_E Generation Complete! ===")

if __name__ == "__main__":
    # You can specify a custom number of synthetic ALF_E values to generate
    # main(num_synthetic_alfs=1000000)  # Generate 1 million synthetic ALF_E values
    
    # Or let it auto-determine based on real ALF_E count
    main()
