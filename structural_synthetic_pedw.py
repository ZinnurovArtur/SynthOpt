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
CHUNK_SIZE = 10000  # Increased for better efficiency

# Add a lock for thread-safe progress file updates
progress_lock = threading.Lock()

# Instantiate the adapter
adapter = TrinoDBAdapter(username="zinnurar", host="trino.feasibility.sail.pk.serp.ac.uk")

# Cache for column names to avoid repeated database calls
_column_names_cache = None

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

@lru_cache(maxsize=1)
def get_synthetic_table_row_count():
    """Get the actual number of rows in the synthetic table (cached)"""
    cursor = adapter.get_cursor()
    cursor.execute("SELECT COUNT(*) FROM iceberg.arthur.pedw_admissions_20231127_structural_synthetic")
    count = cursor.fetchone()[0]
    cursor.close()
    print(f"Found {count} total rows in synthetic table")
    return count

def collect_metadata_sample():
    """Collect metadata from a single sample of 100k rows"""
    print("=== Collecting Structural Metadata from 100k Sample ===")
        
    # Collect 100k rows for metadata
    SAMPLE_SIZE = 10000
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

def get_existing_data_chunk(offset, chunk_size):
    """Get existing data chunk from the synthetic table"""
    try:
        cursor = adapter.get_cursor()
        # First get the column names from the synthetic table
        cursor.execute("SELECT * FROM iceberg.arthur.pedw_admissions_20231127_structural_synthetic LIMIT 1")
        column_names = [desc[0] for desc in cursor.description]
        cursor.close()
        
        # Build a query that explicitly selects all columns except any potential row_number column
        cursor = adapter.get_cursor()
        cursor.execute(f'''
            SELECT * FROM (
                SELECT *, row_number() OVER (ORDER BY alf_e) as temp_row_num
                FROM iceberg.arthur.pedw_admissions_20231127_structural_synthetic
            ) t
            WHERE temp_row_num > {offset} AND temp_row_num <= {offset + chunk_size}
        ''')
        rows = cursor.fetchall()
        cursor.close()
        
        if not rows:
            return None
        
        # Add the temp_row_num column to the column names to match the query result
        column_names_with_temp = column_names + ['temp_row_num']
        
        # Create DataFrame and then remove the temp_row_num column
        df = pd.DataFrame.from_records(rows, columns=column_names_with_temp)
        df = df.drop('temp_row_num', axis=1)
        
        return df
    except Exception as e:
        print(f"Error getting existing data for offset {offset}: {e}")
        return None

def check_table_health():
    """Check if the synthetic table is in a healthy state"""
    try:
        cursor = adapter.get_cursor()
        cursor.execute("SELECT COUNT(*) FROM iceberg.arthur.pedw_admissions_20231127_structural_synthetic LIMIT 1")
        cursor.fetchone()
        cursor.close()
        return True
    except Exception as e:
        print(f"Table health check failed: {e}")
        return False

def update_synthetic_table_with_new_data(metadata, offset, chunk_size, first_chunk, total_rows):
    """Update synthetic table with new synthetic data generated from real table metadata"""
    # Calculate how many rows to process for this chunk
    remaining_rows = total_rows - offset
    rows_to_process = min(chunk_size, remaining_rows)
    
    if rows_to_process <= 0:
        return 0
    
    # Get existing data from synthetic table
    existing_data = get_existing_data_chunk(offset, rows_to_process)
    if existing_data is None:
        print(f"No existing data found for offset {offset}")
        return 0
    
    # Generate new synthetic data using real table metadata
    new_synthetic_data = generate_structural_synthetic_data(metadata, num_records=rows_to_process)
    
    # Update the synthetic table in place with new data (with enhanced retry logic)
    max_retries = 5  # Increased retries
    for attempt in range(max_retries):
        try:
            # Check table health before attempting update
            if not check_table_health():
                print(f"Table health check failed for offset {offset}, skipping...")
                return 0
            
            # Add longer delay between attempts to allow metadata to stabilize
            if attempt > 0:
                delay = random.uniform(5, 15)  # Longer delays for metadata conflicts
                print(f"Waiting {delay:.1f} seconds before retry {attempt + 1}...")
                time.sleep(delay)
            
            adapter.update_existing_table('pedw_admissions_20231127_structural_synthetic', new_synthetic_data, schema='iceberg.arthur', identifier_column='alf_e')
            
            with progress_lock:
                save_offset(offset + rows_to_process)
            
            print(f"Updated synthetic table for rows {offset+1}-{offset+rows_to_process}")
            return rows_to_process
            
        except Exception as e:
            error_msg = str(e)
            if "ICEBERG_COMMIT_ERROR" in error_msg or "conflicting delete files" in error_msg.lower() or "metadata location" in error_msg.lower():
                print(f"Attempt {attempt + 1}/{max_retries} failed for offset {offset}: Iceberg metadata error - {error_msg}")
                if attempt < max_retries - 1:
                    # Longer delay for metadata conflicts
                    delay = random.uniform(10, 30)
                    print(f"Waiting {delay:.1f} seconds for metadata to stabilize...")
                    time.sleep(delay)
                    continue
                else:
                    print(f"Failed to update after {max_retries} attempts for offset {offset}")
                    print("This chunk will be skipped. You may need to restart the process later.")
                    return 0
            else:
                print(f"Unexpected error for offset {offset}: {error_msg}")
                return 0
    
    return 0

def update_synthetic_table_completely(metadata):
    """Update the entire synthetic table with new synthetic data generated from real table metadata"""
    print("=== Updating Synthetic Table with New Synthetic Data ===")
    
    offset = get_last_offset()
    first_chunk = (offset == 0)
    max_workers = 1  # Reduced from 5 to minimize conflicts
    TOTAL_ROWS = get_synthetic_table_row_count()
    
    print(f"Starting update from offset {offset} for {TOTAL_ROWS} total rows")
    print("Generating new synthetic data using real table metadata...")
    print(f"Using {max_workers} workers to minimize Iceberg conflicts")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        chunk_offsets = list(range(offset, TOTAL_ROWS, CHUNK_SIZE))
        
        for i, chunk_offset in enumerate(chunk_offsets):
            is_first = first_chunk and (i == 0)
            futures.append(executor.submit(update_synthetic_table_with_new_data, metadata, chunk_offset, CHUNK_SIZE, is_first, TOTAL_ROWS))
        
        for future in as_completed(futures):
            rows_processed = future.result()
            # Add small delay between chunk completions to reduce conflicts
            time.sleep(random.uniform(0.1, 0.5))

    """Update existing data with synthetic values"""
    print("=== Updating Existing PEDW Data with Synthetic Values ===")
    
    offset = get_last_offset()
    first_chunk = (offset == 0)
    max_workers = 5
    TOTAL_ROWS = get_synthetic_table_row_count()
    
    print(f"Starting update from offset {offset} for {TOTAL_ROWS} total rows")
    if columns_to_update:
        print(f"Updating columns: {columns_to_update}")
    else:
        print("Updating all columns except identifiers")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        chunk_offsets = list(range(offset, TOTAL_ROWS, CHUNK_SIZE))
        
        for i, chunk_offset in enumerate(chunk_offsets):
            is_first = first_chunk and (i == 0)
            futures.append(executor.submit(update_and_save_chunk, metadata, chunk_offset, CHUNK_SIZE, is_first, TOTAL_ROWS, columns_to_update))
        
        for future in as_completed(futures):
            rows_processed = future.result()

def generate_and_save_chunk(metadata, offset, chunk_size, first_chunk, total_rows):
    """Generate synthetic data for a chunk and save it"""
    # Calculate how many rows to generate for this chunk
    remaining_rows = total_rows - offset
    rows_to_generate = min(chunk_size, remaining_rows)
    
    if rows_to_generate <= 0:
        return 0
    
    # Generate synthetic data using metadata
    STRUCT_SYNTHETIC_DATA = generate_structural_synthetic_data(metadata, num_records=rows_to_generate)
    
    # Save synthetic data, append if not first chunk
    adapter.save_synthetic_table('pedw_admissions_20231127_structural_synthetic', STRUCT_SYNTHETIC_DATA, append=not first_chunk)
    
    with progress_lock:
        save_offset(offset + rows_to_generate)
    
    print(f"Generated and saved synthetic data for rows {offset+1}-{offset+rows_to_generate}")
    return rows_to_generate

def generate_synthetic_data(metadata):
    """Generate completely new synthetic data using the collected metadata"""
    print("=== Generating New PEDW Synthetic Data ===")
    
    offset = get_last_offset()
    first_chunk = (offset == 0)
    max_workers = 2
    TOTAL_ROWS = get_admissions_row_count()
    
    print(f"Starting generation from offset {offset} for {TOTAL_ROWS} total rows")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        chunk_offsets = list(range(offset, TOTAL_ROWS, CHUNK_SIZE))
        
        for i, chunk_offset in enumerate(chunk_offsets):
            is_first = first_chunk and (i == 0)
            futures.append(executor.submit(generate_and_save_chunk, metadata, chunk_offset, CHUNK_SIZE, is_first, TOTAL_ROWS))
        
        for future in as_completed(futures):
            rows_processed = future.result()

def main():
    """Main function for PEDW data processing with synthetic values"""
    print("=== Starting PEDW Data Processing with Synthetic Values ===")
    
    # Collect metadata from 100k sample from the REAL table
    metadata = collect_metadata_sample()

    # Generate new synthetic data using real table metadata
    generate_synthetic_data(metadata)
    
    print("=== PEDW Data Processing Complete! ===")

if __name__ == "__main__":
    main()

