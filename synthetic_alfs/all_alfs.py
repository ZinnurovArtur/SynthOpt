import os
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import random
from functools import lru_cache
import pandas as pd
from synthopt.process.db_adapter import TrinoDBAdapter

# Progress tracking file\
USERNAME = "username"  # Replace with your Trino username
PROGRESS_FILE = "alfs_progress.txt"
CHUNK_SIZE = 100_000

TARGET_TABLE = "iceberg.arthur.all_distinct_alfs"

# Add a lock for thread-safe progress file updates
progress_lock = threading.Lock()

# Instantiate the adapter
adapter = TrinoDBAdapter(
    username=USERNAME,
    host="trino.feasibility.sail.pk.serp.ac.uk",
)


def ensure_target_table():
    """
    Create target table for storing distinct alf_e values or apply changes to the table
    """
    columns_sql = """
        "alf_e" BIGINT
    """
    adapter.ensure_table(TARGET_TABLE, columns_sql)


def post_maintenance():
    """Run maintenance operations on the target table"""
    cur = adapter.get_cursor()
    try:
        cur.execute(
            f"ALTER TABLE {TARGET_TABLE} EXECUTE optimize(file_size_threshold => '256MB')"
        )
        cur.execute(f"ALTER TABLE {TARGET_TABLE} EXECUTE optimize_manifests")
        cur.execute(
            f"ALTER TABLE {TARGET_TABLE} EXECUTE expire_snapshots(retention_threshold => '2d')"
        )
        cur.execute(
            f"ALTER TABLE {TARGET_TABLE} EXECUTE remove_orphan_files(retention_threshold => '2d')"
        )
    finally:
        cur.close()


def get_distinct_alfs_from_table(table_name, chunk_size=CHUNK_SIZE):
    """
    Get distinct alf_e values from a specific table in chunks
    """
    print(f"Collecting distinct alf_e values from {table_name}")

    cursor = adapter.get_cursor()
    try:
        # Get total count of distinct alf_e values
        cursor.execute(f"SELECT COUNT(DISTINCT alf_e) FROM {table_name}")
        total_distinct = cursor.fetchone()[0]
        print(f"Found {total_distinct} distinct alf_e values in {table_name}")

        if total_distinct == 0:
            print(f"No alf_e values found in {table_name}")
            return set()

        # Collect distinct alf_e values in chunks
        all_alfs = set()
        offset = 0

        while offset < total_distinct:
            limit = min(chunk_size, total_distinct - offset)
            print(f"Collecting chunk {offset}-{offset + limit} from {table_name}")

            cursor.execute(
                f"""
                SELECT DISTINCT alf_e 
                FROM {table_name} 
                WHERE alf_e IS NOT NULL
                ORDER BY alf_e
                OFFSET {offset} LIMIT {limit} 
            """
            )

            chunk_alfs = {row[0] for row in cursor.fetchall() if row[0] is not None}
            all_alfs.update(chunk_alfs)

            offset += limit

            # Progress update
            print(
                f"Collected {len(all_alfs)} unique alf_e values so far from {table_name}"
            )

        print(
            f"Completed collection from {table_name}: {len(all_alfs)} unique alf_e values"
        )
        return all_alfs

    finally:
        cursor.close()


def collect_all_distinct_alfs(table_names):
    """
    Collect distinct alf_e values from all specified tables
    """
    print("=== Collecting Distinct ALF_E Values from All Tables ===")

    all_distinct_alfs = set()

    for table_name in table_names:
        try:
            table_alfs = get_distinct_alfs_from_table(table_name)
            all_distinct_alfs.update(table_alfs)
            print(
                f"Total unique alf_e values collected so far: {len(all_distinct_alfs)}"
            )
        except Exception as e:
            print(f"Error collecting from {table_name}: {e}")
            continue

    print(
        f"Final collection complete: {len(all_distinct_alfs)} total unique alf_e values"
    )
    return all_distinct_alfs


def save_alfs_to_table(alfs_set):
    """
    Save all distinct alf_e values to the target table
    """
    print("=== Saving Distinct ALF_E Values to Target Table ===")

    ensure_target_table()

    # Convert set to sorted list for consistent ordering
    alfs_list = sorted(list(alfs_set))
    total_alfs = len(alfs_list)

    print(f"Saving {total_alfs} distinct alf_e values to {TARGET_TABLE}")

    cur = adapter.get_cursor()
    try:
        # Schema & writer settings
        cur.execute(f"USE iceberg.{USERNAME}")
        adapter.set_writer_settings(cur)

        committed_until = 0
        rows_since_maintenance = 0

        for offset in range(0, total_alfs, CHUNK_SIZE):
            remaining = total_alfs - offset
            rows_to_save = min(CHUNK_SIZE, remaining)
            print(f"Saving chunk offset={offset}, rows={rows_to_save}")

            # Prepare data for this chunk
            chunk_alfs = alfs_list[offset : offset + rows_to_save]
            data_dict = {"alf_e": chunk_alfs}

            # Insert chunk
            adapter.insert_rows_batch(
                full_table_name=TARGET_TABLE,
                data=data_dict,
                columns=["alf_e"],
                cursor=cur,
                max_tuples_per_insert=500_000,
                max_sql_chars=1_000_000,
            )

            committed_until = offset + rows_to_save
            rows_since_maintenance += rows_to_save

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

    print("=== Save complete ===")


def get_last_offset():
    """Get the last saved offset from progress file"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            try:
                return int(f.read().strip())
            except Exception:
                return 0
    return 0


def save_offset(offset):
    """Save the current offset to progress file"""
    with open(PROGRESS_FILE, "w") as f:
        f.write(str(offset))


def main(table_names):
    """
    Main function to collect distinct alf_e values from multiple tables

    Args:
        table_names (list): List of table names to collect alf_e values from
    """
    print("=== Starting ALF_E Collection from Multiple Tables ===")
    print(f"Target tables: {table_names}")

    # Collect all distinct alf_e values
    all_distinct_alfs = collect_all_distinct_alfs(table_names)

    if not all_distinct_alfs:
        print("No alf_e values found in any of the specified tables")
        return

    # Save to target table
    save_alfs_to_table(all_distinct_alfs)

    print("=== ALF_E Collection Complete! ===")
    print(f"Total unique alf_e values saved: {len(all_distinct_alfs)}")


if __name__ == "__main__":
    tables_to_process = [
        "iceberg.wlgp.gp_event_reformatted_20250401",
        "iceberg.pedw.pedw_admissions_20250505",
        "iceberg.ncch.child_births_20250501",
        "iceberg.eduw.pupil_alf",
        "iceberg.lacw.pupil_alf",
        "iceberg.crcs.pupil_alf",
        # Add more tables as needed
    ]

    main(tables_to_process)
