import os
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from synthopt.process.structural_metadata import process_structural_metadata, process_structural_metadata_sql
from synthopt.generate.structural_synthetic_data import generate_structural_synthetic_data, generate_structural_synthetic_data_from_sql
from pedw import get_table_admisions
from eduw import get_table_pupil_alf
from db_adapter import TrinoDBAdapter
import pandas as pd

# Set date_formats to None as in new_test.py
# You can adjust this if you want to specify formats

date_formats = ["%Y-%m-%d"]

# Progress tracking file
PROGRESS_FILE = 'progress.txt'
METADATA_FILE = 'complete_metadata.pkl'
PUPIL_METADATA_FILE = 'pupil_complete_metadata.pkl'
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

def collect_chunk_metadata(offset, chunk_size):
    """Collect metadata from a single chunk of data"""
    rows = get_table_admisions(adapter, limit=chunk_size, offset=offset)
    if not rows:
        return None
    
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
    DATA = pd.DataFrame.from_records(rows, columns=columns)
    chunk_metadata = process_structural_metadata(DATA, datetime_formats=date_formats)
    
    print(f"Collected metadata from rows {offset+1}-{offset+len(rows)}")
    return chunk_metadata

def collect_pupil_chunk_metadata(offset, chunk_size):
    """Collect metadata from a single chunk of pupil_alf data"""
    rows = get_table_pupil_alf(adapter, limit=chunk_size, offset=offset)
    if not rows:
        return None
    
    # Get column names from the cursor description
    cursor = adapter.get_cursor()
    cursor.execute(f'''
        SELECT * FROM (
            SELECT *, row_number() OVER () as rn
            FROM iceberg.eduw.pupil_alf
        ) t
        WHERE rn = {offset + 1}
    ''')
    columns = [desc[0] for desc in cursor.description]
    cursor.close()
    
    # Convert to DataFrame
    DATA = pd.DataFrame.from_records(rows, columns=columns)
    chunk_metadata = process_structural_metadata(DATA, datetime_formats=date_formats)
    
    print(f"Collected pupil metadata from rows {offset+1}-{offset+len(rows)}")
    return chunk_metadata

def phase1_collect_complete_metadata():
    """Phase 1: Collect complete metadata from all data in parallel"""
    print("=== Phase 1: Collecting Complete Metadata ===")
    
    # Check if metadata already exists
    if os.path.exists(METADATA_FILE):
        print(f"Loading existing metadata from {METADATA_FILE}")
        with open(METADATA_FILE, 'rb') as f:
            return pickle.load(f)
    
    max_workers = 5
    TOTAL_ROWS = 2000000  # Adjust based on your actual data size
    
    all_chunk_metadata = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        chunk_offsets = list(range(0, TOTAL_ROWS, CHUNK_SIZE))
        
        for chunk_offset in chunk_offsets:
            futures.append(executor.submit(collect_chunk_metadata, chunk_offset, CHUNK_SIZE))
        
        for future in as_completed(futures):
            chunk_metadata = future.result()
            if chunk_metadata is not None:
                all_chunk_metadata.append(chunk_metadata)
    
    # Combine all chunk metadata into complete metadata
    print("Combining metadata from all chunks...")
    complete_metadata = all_chunk_metadata[0]  # Start with first chunk
    
    for chunk_metadata in all_chunk_metadata[1:]:
        # Merge metadata (this depends on how your metadata structure works)
        # You might need to implement a custom merge function based on your metadata format
        complete_metadata = merge_metadata(complete_metadata, chunk_metadata)
    
    # Save complete metadata
    print(f"Saving complete metadata to {METADATA_FILE}")
    with open(METADATA_FILE, 'wb') as f:
        pickle.dump(complete_metadata, f)
    
    return complete_metadata

def get_pupil_row_count():
    """Get the actual number of rows in pupil_alf table"""
    cursor = adapter.get_cursor()
    cursor.execute("SELECT COUNT(*) FROM iceberg.eduw.pupil_alf")
    count = cursor.fetchone()[0]
    cursor.close()
    print(f"Found {count} total rows in pupil_alf table")
    return count

def phase1_collect_pupil_metadata():
    """Phase 1b: Collect complete metadata from pupil_alf data"""
    print("=== Phase 1b: Collecting Pupil Metadata ===")
    
    # Check if metadata already exists
    if os.path.exists(PUPIL_METADATA_FILE):
        print(f"Loading existing pupil metadata from {PUPIL_METADATA_FILE}")
        with open(PUPIL_METADATA_FILE, 'rb') as f:
            return pickle.load(f)
    
    max_workers = 5
    TOTAL_PUPIL_ROWS = get_pupil_row_count()  # Get actual count from database
    
    all_chunk_metadata = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        chunk_offsets = list(range(0, TOTAL_PUPIL_ROWS, CHUNK_SIZE))
        
        for chunk_offset in chunk_offsets:
            futures.append(executor.submit(collect_pupil_chunk_metadata, chunk_offset, CHUNK_SIZE))
        
        for future in as_completed(futures):
            chunk_metadata = future.result()
            if chunk_metadata is not None:
                all_chunk_metadata.append(chunk_metadata)
    
    # Combine all chunk metadata into complete metadata
    print("Combining pupil metadata from all chunks...")
    complete_metadata = all_chunk_metadata[0]  # Start with first chunk
    
    for chunk_metadata in all_chunk_metadata[1:]:
        complete_metadata = merge_metadata(complete_metadata, chunk_metadata)
    
    # Save complete metadata
    print(f"Saving complete pupil metadata to {PUPIL_METADATA_FILE}")
    with open(PUPIL_METADATA_FILE, 'wb') as f:
        pickle.dump(complete_metadata, f)
    
    return complete_metadata

def merge_metadata(metadata1, metadata2):
    """Merge two metadata DataFrames from different chunks"""
    if metadata1 is None:
        return metadata2
    if metadata2 is None:
        return metadata1
    
    # Create a copy to avoid modifying the original
    merged = metadata1.copy()
    
    # For each column in metadata2, update the corresponding row in merged
    for idx2, row2 in metadata2.iterrows():
        col_name = row2['variable_name']
        
        # Find corresponding row in merged metadata
        matching_rows = merged[merged['variable_name'] == col_name]
        
        if len(matching_rows) == 0:
            # New column, add it
            merged = pd.concat([merged, pd.DataFrame([row2])], ignore_index=True)
        else:
            # Existing column, merge the information
            idx1 = matching_rows.index[0]
            
            # Merge completeness (weighted average)
            comp1 = merged.at[idx1, 'completeness']
            comp2 = row2['completeness']
            # For now, take the average (you might want to implement proper weighted average based on chunk sizes)
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
            
            # Merge coding (keep the first non-None one, or combine if needed)
            coding1 = merged.at[idx1, 'coding']
            coding2 = row2['coding']
            if coding1 is None and coding2 is not None:
                merged.at[idx1, 'coding'] = coding2
            elif coding1 is not None and coding2 is not None:
                # If both have coding, you might want to merge them
                # For now, keep the first one
                pass
    
    return merged

def generate_and_save_chunk(complete_metadata, offset, chunk_size, first_chunk):
    """Generate synthetic data for a chunk and save it"""
    # Generate synthetic data using complete metadata
    STRUCT_SYNTHETIC_DATA = generate_structural_synthetic_data(complete_metadata, num_records=chunk_size)
    
    # Save synthetic data, append if not first chunk
    adapter.save_synthetic_table('pedw_admissions_20231127_structural_synthetic', STRUCT_SYNTHETIC_DATA, append=not first_chunk)
    
    with progress_lock:
        save_offset(offset + chunk_size)
    
    print(f"Generated and saved synthetic data for rows {offset+1}-{offset+chunk_size}")
    return chunk_size

def phase2_generate_synthetic_data(complete_metadata):
    """Phase 2: Generate synthetic data using complete metadata"""
    print("=== Phase 2: Generating Synthetic Data ===")
    
    offset = get_last_offset()
    first_chunk = (offset == 0)
    max_workers = 5
    TOTAL_ROWS = 2000000  # Adjust based on your actual data size
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        chunk_offsets = list(range(offset, TOTAL_ROWS, CHUNK_SIZE))
        
        for i, chunk_offset in enumerate(chunk_offsets):
            is_first = first_chunk and (i == 0)
            futures.append(executor.submit(generate_and_save_chunk, complete_metadata, chunk_offset, CHUNK_SIZE, is_first))
        
        for future in as_completed(futures):
            rows_processed = future.result()

def extract_alf_e_from_synthetic_admissions():
    """Extract alf_e values from synthetic admissions data for pupil_alf generation"""
    print("=== Extracting alf_e from synthetic admissions ===")
    
    # Query the synthetic admissions table to get all alf_e values
    cursor = adapter.get_cursor()
    cursor.execute("SELECT alf_e FROM iceberg.arthur.pedw_admissions_20231127_structural_synthetic")
    alf_e_values = [row[0] for row in cursor.fetchall()]
    cursor.close()
    
    print(f"Extracted {len(alf_e_values)} alf_e values from synthetic admissions")
    return alf_e_values

def generate_pupil_synthetic_with_referential_integrity(pupil_metadata, available_alf_e_values):
    """Generate synthetic pupil_alf data using alf_e values from synthetic admissions"""
    print("=== Phase 3: Generating Pupil Synthetic Data with Referential Integrity ===")
    
    # Modify the pupil metadata to use available alf_e values
    modified_metadata = pupil_metadata.copy()
    
    # Find the alf_e column in metadata
    alf_e_row = modified_metadata[modified_metadata['variable_name'] == 'alf_e']
    if len(alf_e_row) > 0:
        idx = alf_e_row.index[0]
        # Update the values to use only the available alf_e values from synthetic admissions
        modified_metadata.at[idx, 'values'] = available_alf_e_values
    
    # Generate synthetic pupil data
    pupil_synthetic_data = generate_structural_synthetic_data(modified_metadata, num_records=len(available_alf_e_values))
    
    # Save to pupil_alf synthetic table
    adapter.save_synthetic_table('pupil_alf_structural_synthetic', pupil_synthetic_data, append=False)
    
    print(f"Generated and saved {len(available_alf_e_values)} synthetic pupil records")
    return pupil_synthetic_data

def threading_code():
    """Original threading approach (kept for reference)"""
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

def pupil_alf_extraction():
    """Run only pupil_alf generation using existing synthetic admissions data"""
    print("=== Running Pupil_alf Generation Only ===")
    
    # Check if synthetic admissions data exists
    cursor = adapter.get_cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM iceberg.arthur.pedw_admissions_20231127_structural_synthetic")
        count = cursor.fetchone()[0]
        print(f"Found {count} existing synthetic admissions records")
    except Exception as e:
        print(f"Error: No existing synthetic admissions data found: {e}")
        print("Please run the full pipeline first to generate admissions synthetic data")
        cursor.close()
        return
    cursor.close()
    
    # Now handle pupil_alf with referential integrity
    pupil_metadata = phase1_collect_pupil_metadata()
    available_alf_e_values = extract_alf_e_from_synthetic_admissions()
    generate_pupil_synthetic_with_referential_integrity(pupil_metadata, available_alf_e_values)
    
    print("=== Pupil_alf generation complete! ===")

def main():
    # Phase 1: Collect complete metadata for both tables
    print("=== Starting Complete Synthetic Data Generation ===")
    
    # Generate admissions synthetic data first
    complete_metadata = phase1_collect_complete_metadata()
    phase2_generate_synthetic_data(complete_metadata)
    
    # Now handle pupil_alf with referential integrity
    pupil_metadata = phase1_collect_pupil_metadata()
    available_alf_e_values = extract_alf_e_from_synthetic_admissions()
    generate_pupil_synthetic_with_referential_integrity(pupil_metadata, available_alf_e_values)
    
    print("=== Complete! Both tables generated with referential integrity ===")

if __name__ == "__main__":
    # Uncomment the function you want to run:
    #main()  # Run full pipeline (admissions + pupil)
    pupil_alf_extraction()  # Run only pupil generation using existing admissions