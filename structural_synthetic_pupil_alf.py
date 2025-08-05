import os
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import random
from functools import lru_cache

from synthopt.process.structural_metadata import process_structural_metadata, process_structural_metadata_sql
from synthopt.generate.structural_synthetic_data import generate_structural_synthetic_data, generate_structural_synthetic_data_from_sql
from eduw import get_table_pupil_alf
from db_adapter import TrinoDBAdapter
import pandas as pd

# Set date_formats
date_formats = ["%Y-%m-%d"]

# Progress tracking file
PROGRESS_FILE = 'pupil_progress.txt'
PUPIL_METADATA_FILE = 'pupil_complete_metadata.pkl'
CHUNK_SIZE = 10000  # Increased for better efficiency

# Add a lock for thread-safe progress file updates
progress_lock = threading.Lock()

# Instantiate the adapter
adapter = TrinoDBAdapter(username="zinnurar", host="trino.feasibility.sail.pk.serp.ac.uk")

# Cache for column names to avoid repeated database calls
_pupil_column_names_cache = None

@lru_cache(maxsize=1)
def get_pupil_column_names():
    """Get pupil_alf column names once and cache them"""
    global _pupil_column_names_cache
    if _pupil_column_names_cache is None:
        cursor = adapter.get_cursor()
        cursor.execute(f'''
            SELECT * FROM (
                SELECT *, row_number() OVER () as rn
                FROM iceberg.eduw.pupil_alf
            ) t
            WHERE rn = 1
        ''')
        _pupil_column_names_cache = [desc[0] for desc in cursor.description]
        cursor.close()
    return _pupil_column_names_cache

@lru_cache(maxsize=1)
def get_pupil_row_count():
    """Get the actual number of rows in pupil_alf table (cached)"""
    cursor = adapter.get_cursor()
    cursor.execute("SELECT COUNT(*) FROM iceberg.eduw.pupil_alf")
    count = cursor.fetchone()[0]
    cursor.close()
    print(f"Found {count} total rows in pupil_alf table")
    return count

def collect_pupil_chunk_metadata(offset, chunk_size):
    """Collect metadata from a single chunk of pupil_alf data (optimized)"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Reduced delay for better efficiency
            time.sleep(random.uniform(0.05, 0.2))
            
            rows = get_table_pupil_alf(adapter, limit=chunk_size, offset=offset)
            if not rows:
                return None
            
            # Use cached column names instead of database call
            columns = get_pupil_column_names()
            
            # Convert to DataFrame more efficiently
            DATA = pd.DataFrame.from_records(rows, columns=columns)
            chunk_metadata = process_structural_metadata(DATA, datetime_formats=date_formats)
            
            print(f"Collected pupil metadata from rows {offset+1}-{offset+len(rows)}")
            return chunk_metadata
            
        except Exception as e:
            print(f"Error collecting pupil metadata for offset {offset} (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.5, 1.5))  # Reduced retry delay
            else:
                print(f"Failed to collect pupil metadata for offset {offset} after {max_retries} attempts")
                return None

def phase1_collect_pupil_metadata():
    """Phase 1: Collect complete metadata from pupil_alf data (optimized)"""
    print("=== Phase 1: Collecting Pupil Metadata (Sample) ===")
    
    # Check if metadata already exists
    if os.path.exists(PUPIL_METADATA_FILE):
        print(f"Loading existing pupil metadata from {PUPIL_METADATA_FILE}")
        with open(PUPIL_METADATA_FILE, 'rb') as f:
            return pickle.load(f)
    
    max_workers = 8  # Increased workers for better parallelism
    TOTAL_PUPIL_ROWS = get_pupil_row_count()
    
    # Use only 50K rows for metadata collection (much faster)
    METADATA_SAMPLE_SIZE = min(50000, TOTAL_PUPIL_ROWS)
    print(f"Collecting pupil metadata from {METADATA_SAMPLE_SIZE} rows (out of {TOTAL_PUPIL_ROWS} total)")
    
    all_chunk_metadata = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        chunk_offsets = list(range(0, METADATA_SAMPLE_SIZE, CHUNK_SIZE))
        
        for chunk_offset in chunk_offsets:
            futures.append(executor.submit(collect_pupil_chunk_metadata, chunk_offset, CHUNK_SIZE))
        
        # Process results as they complete
        for future in as_completed(futures):
            chunk_metadata = future.result()
            if chunk_metadata is not None:
                all_chunk_metadata.append(chunk_metadata)
    
    # Optimized metadata merging
    print("Combining pupil metadata from all chunks...")
    if not all_chunk_metadata:
        raise ValueError("No pupil metadata collected!")
    
    complete_metadata = all_chunk_metadata[0]
    
    # Batch merge for better performance
    for chunk_metadata in all_chunk_metadata[1:]:
        complete_metadata = merge_metadata(complete_metadata, chunk_metadata)
    
    # Save complete metadata
    print(f"Saving complete pupil metadata to {PUPIL_METADATA_FILE}")
    with open(PUPIL_METADATA_FILE, 'wb') as f:
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

@lru_cache(maxsize=1)
def extract_alf_e_from_synthetic_admissions():
    """Extract alf_e values from synthetic admissions data (cached)"""
    print("=== Extracting alf_e from synthetic admissions ===")
    
    # Query the synthetic admissions table to get all alf_e values
    cursor = adapter.get_cursor()
    cursor.execute("SELECT alf_e FROM iceberg.arthur.pedw_admissions_20231127_structural_synthetic")
    alf_e_values = [row[0] for row in cursor.fetchall()]
    cursor.close()
    
    print(f"Extracted {len(alf_e_values)} alf_e values from synthetic admissions")
    return alf_e_values

@lru_cache(maxsize=1)
def extract_alf_e_from_original_admissions():
    """Extract alf_e values from original pedw_admissions data (cached)"""
    print("=== Extracting alf_e from original admissions ===")
    
    # Query the original admissions table to get all alf_e values
    cursor = adapter.get_cursor()
    cursor.execute("SELECT alf_e FROM iceberg.pedw.pedw_admissions_20231127")
    alf_e_values = [row[0] for row in cursor.fetchall()]
    cursor.close()
    
    print(f"Extracted {len(alf_e_values)} alf_e values from original admissions")
    return alf_e_values

def generate_pupil_synthetic_with_referential_integrity(pupil_metadata, available_alf_e_values):
    """Generate synthetic pupil_alf data using alf_e values from admissions (optimized)"""
    print("=== Phase 2: Generating Pupil Synthetic Data with Referential Integrity ===")
    
    # Get the actual pupil count to determine how many records to generate
    original_pupil_count = get_pupil_row_count()
    
    # Modify the pupil metadata to use available alf_e values
    modified_metadata = pupil_metadata.copy()
    
    # Find the alf_e column in metadata
    alf_e_row = modified_metadata[modified_metadata['variable_name'] == 'alf_e']
    if len(alf_e_row) > 0:
        idx = alf_e_row.index[0]
        # Update the values to use only the available alf_e values from admissions
        modified_metadata.at[idx, 'values'] = available_alf_e_values
    
    # Generate synthetic pupil data - use the actual pupil count
    pupil_synthetic_data = generate_structural_synthetic_data(modified_metadata, num_records=original_pupil_count)
    
    # Save to pupil_alf synthetic table
    adapter.save_synthetic_table('pupil_alf_structural_synthetic', pupil_synthetic_data, append=False)
    
    print(f"Generated and saved {original_pupil_count} synthetic pupil records")
    return pupil_synthetic_data

def run_with_synthetic_admissions():
    """Run pupil_alf generation using existing synthetic admissions data"""
    print("=== Running Pupil_alf Generation with Synthetic Admissions ===")
    
    # Check if synthetic admissions data exists
    cursor = adapter.get_cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM iceberg.arthur.pedw_admissions_20231127_structural_synthetic")
        count = cursor.fetchone()[0]
        print(f"Found {count} existing synthetic admissions records")
        available_alf_e_values = extract_alf_e_from_synthetic_admissions()
    except Exception as e:
        print(f"Error: No existing synthetic admissions data found: {e}")
        print("Please run structural_synthetic_pedw.py first to generate admissions synthetic data")
        cursor.close()
        return
    cursor.close()
    
    # Generate pupil_alf with referential integrity
    pupil_metadata = phase1_collect_pupil_metadata()
    generate_pupil_synthetic_with_referential_integrity(pupil_metadata, available_alf_e_values)
    
    print("=== Pupil_alf generation complete! ===")

def run_with_original_admissions():
    """Run pupil_alf generation using original admissions data"""
    print("=== Running Pupil_alf Generation with Original Admissions ===")
    
    # Extract alf_e from original admissions
    available_alf_e_values = extract_alf_e_from_original_admissions()
    
    # Generate pupil_alf with referential integrity
    pupil_metadata = phase1_collect_pupil_metadata()
    generate_pupil_synthetic_with_referential_integrity(pupil_metadata, available_alf_e_values)
    
    print("=== Pupil_alf generation complete using original admissions! ===")

def run_smart_detection():
    """Run pupil_alf generation with smart detection of available admissions data"""
    print("=== Running Pupil_alf Generation with Smart Detection ===")
    
    # Check if synthetic admissions data exists
    cursor = adapter.get_cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM iceberg.arthur.pedw_admissions_20231127_structural_synthetic")
        count = cursor.fetchone()[0]
        print(f"Found {count} existing synthetic admissions records")
        # Use synthetic admissions if available
        available_alf_e_values = extract_alf_e_from_synthetic_admissions()
    except Exception as e:
        print(f"No existing synthetic admissions data found: {e}")
        print("Using original admissions data for alf_e extraction...")
        # Use original admissions data
        available_alf_e_values = extract_alf_e_from_original_admissions()
    cursor.close()
    
    # Generate pupil_alf with referential integrity
    pupil_metadata = phase1_collect_pupil_metadata()
    generate_pupil_synthetic_with_referential_integrity(pupil_metadata, available_alf_e_values)
    
    print("=== Pupil_alf generation complete! ===")

def main():
    """Main function for pupil_alf synthetic data generation"""
    print("=== Starting Pupil_alf Synthetic Data Generation ===")
    
    # Phase 1: Collect pupil metadata
    pupil_metadata = phase1_collect_pupil_metadata()
    
    # Phase 2: Generate pupil synthetic data with smart detection
    run_smart_detection()
    
    print("=== Pupil_alf Synthetic Data Generation Complete! ===")

if __name__ == "__main__":
    # Uncomment the function you want to run:
    main()  # Run with smart detection (recommended)
    # run_with_synthetic_admissions()  # Run only with synthetic admissions
    # run_with_original_admissions()  # Run only with original admissions