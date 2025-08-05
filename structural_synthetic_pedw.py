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
METADATA_FILE = 'pedw_complete_metadata.pkl'
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
        cursor.execute(f'''
            SELECT * FROM (
                SELECT *, row_number() OVER () as rn
                FROM iceberg.pedw.pedw_admissions_20231127
            ) t
            WHERE rn = 1
        ''')
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

def collect_chunk_metadata(offset, chunk_size):
    """Collect metadata from a single chunk of data (optimized)"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Reduced delay for better efficiency
            time.sleep(random.uniform(0.05, 0.2))
            
            rows = get_table_admisions(adapter, limit=chunk_size, offset=offset)
            if not rows:
                return None
            
            # Use cached column names instead of database call
            columns = get_column_names()
            
            # Convert to DataFrame more efficiently
            DATA = pd.DataFrame.from_records(rows, columns=columns)
            
            # Process metadata
            chunk_metadata = process_structural_metadata(DATA, datetime_formats=date_formats)
            
            print(f"Collected metadata from rows {offset+1}-{offset+len(rows)}")
            return chunk_metadata
            
        except Exception as e:
            print(f"Error collecting metadata for offset {offset} (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.5, 1.5))  # Reduced retry delay
            else:
                print(f"Failed to collect metadata for offset {offset} after {max_retries} attempts")
                return None

def phase1_collect_complete_metadata():
    """Phase 1: Collect complete metadata from a sample of data in parallel (optimized)"""
    print("=== Phase 1: Collecting Complete PEDW Metadata (Sample) ===")
    
    # Check if metadata already exists
    if os.path.exists(METADATA_FILE):
        print(f"Loading existing metadata from {METADATA_FILE}")
        with open(METADATA_FILE, 'rb') as f:
            return pickle.load(f)
    
    max_workers = 8  # Increased workers for better parallelism
    TOTAL_ROWS = get_admissions_row_count()
    
    # Use only 50K rows for metadata collection (much faster)
    METADATA_SAMPLE_SIZE = min(50000, TOTAL_ROWS)
    print(f"Collecting metadata from {METADATA_SAMPLE_SIZE} rows (out of {TOTAL_ROWS} total)")
    
    all_chunk_metadata = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        chunk_offsets = list(range(0, METADATA_SAMPLE_SIZE, CHUNK_SIZE))
        
        for chunk_offset in chunk_offsets:
            futures.append(executor.submit(collect_chunk_metadata, chunk_offset, CHUNK_SIZE))
        
        # Process results as they complete
        for future in as_completed(futures):
            chunk_metadata = future.result()
            if chunk_metadata is not None:
                all_chunk_metadata.append(chunk_metadata)
    
    # Optimized metadata merging
    print("Combining metadata from all chunks...")
    if not all_chunk_metadata:
        raise ValueError("No metadata collected!")
    
    complete_metadata = all_chunk_metadata[0]
    
    # Batch merge for better performance
    for chunk_metadata in all_chunk_metadata[1:]:
        complete_metadata = merge_metadata(complete_metadata, chunk_metadata)
    
    # Save complete metadata
    print(f"Saving complete metadata to {METADATA_FILE}")
    with open(METADATA_FILE, 'wb') as f:
        pickle.dump(complete_metadata, f)
    
    return complete_metadata

def merge_metadata(metadata1, metadata2):
    """Merge two metadata DataFrames from different chunks (optimized)"""
    if metadata1 is None:
        return metadata2
    if metadata2 is None:
        return metadata1
    
    # Create a copy to avoid modifying the original
    merged = metadata1.copy()
    
    # Create index for faster lookups
    merged_index = merged.set_index('variable_name')
    
    # For each column in metadata2, update the corresponding row in merged
    for idx2, row2 in metadata2.iterrows():
        col_name = row2['variable_name']
        
        if col_name in merged_index.index:
            # Existing column, merge the information
            idx1 = merged[merged['variable_name'] == col_name].index[0]
            
            # Merge completeness (weighted average)
            comp1 = merged.at[idx1, 'completeness']
            comp2 = row2['completeness']
            merged.at[idx1, 'completeness'] = (comp1 + comp2) / 2
            
            # Merge values based on datatype
            datatype = row2['datatype']
            values1 = merged.at[idx1, 'values']
            values2 = row2['values']
            
            if datatype in ['integer', 'float', 'datetime']:
                # For numerical/datetime: expand the range
                if values1 is not None and values2 is not None:
                    if isinstance(values1, tuple) and isinstance(values2, tuple):
                        min_val = min(values1[0], values2[0])
                        max_val = max(values1[1], values2[1])
                        merged.at[idx1, 'values'] = (min_val, max_val)
            elif datatype in ['categorical string', 'categorical integer', 'categorical float']:
                # For categorical: combine unique values
                if values1 is not None and values2 is not None:
                    if isinstance(values1, list) and isinstance(values2, list):
                        combined_values = list(set(values1 + values2))
                        merged.at[idx1, 'values'] = combined_values
            
            # Merge coding (keep the first non-None one)
            coding1 = merged.at[idx1, 'coding']
            coding2 = row2['coding']
            if coding1 is None and coding2 is not None:
                merged.at[idx1, 'coding'] = coding2
        else:
            # New column, add it
            merged = pd.concat([merged, pd.DataFrame([row2])], ignore_index=True)
    
    return merged

def generate_and_save_chunk(complete_metadata, offset, chunk_size, first_chunk, total_rows):
    """Generate synthetic data for a chunk and save it (optimized)"""
    # Calculate how many rows to generate for this chunk
    remaining_rows = total_rows - offset
    rows_to_generate = min(chunk_size, remaining_rows)
    
    if rows_to_generate <= 0:
        return 0
    
    # Generate synthetic data using complete metadata
    STRUCT_SYNTHETIC_DATA = generate_structural_synthetic_data(complete_metadata, num_records=rows_to_generate)
    
    # Save synthetic data, append if not first chunk
    adapter.save_synthetic_table('pedw_admissions_20231127_structural_synthetic', STRUCT_SYNTHETIC_DATA, append=not first_chunk)
    
    with progress_lock:
        save_offset(offset + rows_to_generate)
    
    print(f"Generated and saved synthetic data for rows {offset+1}-{offset+rows_to_generate}")
    return rows_to_generate

def phase2_generate_synthetic_data(complete_metadata):
    """Phase 2: Generate synthetic data using complete metadata (optimized)"""
    print("=== Phase 2: Generating PEDW Synthetic Data ===")
    
    offset = get_last_offset()
    first_chunk = (offset == 0)
    max_workers = 8  # Increased workers for better parallelism
    TOTAL_ROWS = get_admissions_row_count()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        chunk_offsets = list(range(offset, TOTAL_ROWS, CHUNK_SIZE))
        
        for i, chunk_offset in enumerate(chunk_offsets):
            is_first = first_chunk and (i == 0)
            futures.append(executor.submit(generate_and_save_chunk, complete_metadata, chunk_offset, CHUNK_SIZE, is_first, TOTAL_ROWS))
        
        for future in as_completed(futures):
            rows_processed = future.result()

def main():
    """Main function for PEDW synthetic data generation"""
    print("=== Starting PEDW Synthetic Data Generation ===")
    
    # Phase 1: Collect complete metadata
    complete_metadata = phase1_collect_complete_metadata()
    
    # Phase 2: Generate synthetic data
    phase2_generate_synthetic_data(complete_metadata)
    
    print("=== PEDW Synthetic Data Generation Complete! ===")

if __name__ == "__main__":
    main()
