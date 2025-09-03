import os
import threading
import pandas as pd
import datetime as dt

from synthopt.process.structural_metadata import process_structural_metadata
from synthopt.generate.structural_synthetic_data import (
    generate_structural_synthetic_data,
)

DEFAULT_DATE_FORMATS = ["%Y-%m-%d"]


class IcebergSyntheticLoader:
    """
    Reusable synthetic data loader for Trino+Iceberg.

    You provide:
      - adapter: TrinoDBAdapter (must expose get_cursor(), set_writer_settings(), ensure_table(), insert_rows_batch())
      - target_table: fully-qualified, e.g. "iceberg.arthur.my_target"
      - chunk_size: rows per generation round
      - progress_file: path to save last committed offset (resumability)
      - fn_rowcount(adapter) -> int
      - fn_sample(adapter, limit:int, offset:int) -> list[tuple] or list[dict]
      - fn_columns(adapter) -> list[str]
      - fn_process_metadata(df: DataFrame, datetime_formats: list[str]) -> any
      - fn_generate(metadata, num_records:int) -> DataFrame
    """

    def __init__(
        self,
        adapter,
        target_table: str,
        *,
        schema_qualified: str = "iceberg.arthur",
        chunk_size: int = 250_000,
        maintenance_every_rows: int = 1_000_000,
        progress_file: str = "loader_progress.txt",
        datetime_formats=None,
        # pluggable functions:
        fn_rowcount=None,
        fn_sample=None,
        fn_columns=None,
        fn_process_metadata=None,
        fn_generate=None,
    ):
        self.adapter = adapter
        self.target_table = target_table
        self.schema_qualified = schema_qualified
        self.chunk_size = chunk_size
        self.maintenance_every_rows = maintenance_every_rows
        self.progress_file = progress_file
        self.datetime_formats = datetime_formats or DEFAULT_DATE_FORMATS

        # required callables
        assert (
            fn_rowcount
            and fn_sample
            and fn_columns
            and fn_process_metadata
            and fn_generate
        ), "You must provide fn_rowcount, fn_sample, fn_columns, fn_process_metadata, fn_generate"

        self.fn_rowcount = fn_rowcount
        self.fn_sample = fn_sample
        self.fn_columns = fn_columns
        self.fn_process_metadata = fn_process_metadata
        self.fn_generate = fn_generate

        self._progress_lock = threading.Lock()

    # ---------- progress ----------
    def _get_last_offset(self) -> int:
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, "r") as f:
                    return int(f.read().strip())
            except Exception:
                return 0
        return 0

    def _save_offset(self, offset: int):
        with self._progress_lock:
            with open(self.progress_file, "w") as f:
                f.write(str(offset))

    # ---------- maintenance ----------
    def post_maintenance(self):
        cur = self.adapter.get_cursor()
        try:
            cur.execute(
                f"ALTER TABLE {self.target_table} EXECUTE optimize(file_size_threshold => '256MB')"
            )
            cur.execute(f"ALTER TABLE {self.target_table} EXECUTE optimize_manifests")
            cur.execute(
                f"ALTER TABLE {self.target_table} EXECUTE expire_snapshots(retention_threshold => '2d')"
            )
            cur.execute(
                f"ALTER TABLE {self.target_table} EXECUTE remove_orphan_files(retention_threshold => '2d')"
            )
        finally:
            cur.close()

    # ---------- metadata ----------
    def collect_metadata_sample(self, sample_size: int = 100_000):
        print(
            f"=== Collecting Structural Metadata from {sample_size:,} sample rows ==="
        )
        rows = self.fn_sample(self.adapter, limit=sample_size, offset=0)
        if not rows:
            raise ValueError("No rows returned by fn_sample")

        cols = self.fn_columns(self.adapter)
        df = pd.DataFrame.from_records(rows, columns=cols)
        metadata = self.fn_process_metadata(df, datetime_formats=self.datetime_formats)
        print(f"Collected metadata from {len(df):,} rows.")
        return metadata

    # ---------- ensure table ----------
    def _get_trino_type(self, dtype):
        """Convert pandas dtype to Trino SQL type"""
        if pd.api.types.is_integer_dtype(dtype):
            return "BIGINT"
        elif pd.api.types.is_float_dtype(dtype):
            return "DOUBLE"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "TIMESTAMP"
        elif pd.api.types.is_bool_dtype(dtype):
            return "BOOLEAN"
        else:
            return "VARCHAR"

    def ensure_target_table(self, metadata=None):
        """
        Create target table if it doesn't exist, using metadata to determine column types.
        If metadata is None, will generate a small sample to determine types.
        """
        if metadata is None:
            # Generate a small sample to determine column types
            sample_df = self.fn_generate(metadata, num_records=1)
        else:
            # Generate a sample using provided metadata
            sample_df = self.fn_generate(metadata, num_records=1)

        # Create column definitions from DataFrame types
        column_defs = []
        for col_name, dtype in sample_df.dtypes.items():
            sql_type = self._get_trino_type(dtype)
            column_defs.append(f'"{col_name}" {sql_type}')

        columns_sql = ",\n    ".join(column_defs)
        self.adapter.ensure_table(self.target_table, columns_sql=columns_sql)

    # ---------- main load ----------
    def load(
        self,
        metadata,
        *,
        max_sql_chars: int = 900_000,
        max_tuples_per_insert: int = 250_000,
    ):
        """
        Autocommit mode (no explicit transactions): large INSERTs and periodic maintenance.
        If your environment supports explicit transactions and you prefer coalescing snapshots,
        wrap multiple calls to insert_rows_batch with START TRANSACTION/COMMIT outside this method.
        """
        total_rows = self.fn_rowcount(self.adapter)
        start_offset = self._get_last_offset()

        print("=== Starting synthetic load ===")
        print(f"Target table: {self.target_table}")
        print(f"Resuming at offset {start_offset:,} of {total_rows:,}")
        print(
            f"Chunk size = {self.chunk_size:,}, maintenance window = {self.maintenance_every_rows:,}"
        )

        cur = self.adapter.get_cursor()
        try:
            # Set context & writer behavior
            cur.execute(f"USE {self.schema_qualified}")
            self.adapter.set_writer_settings(cur)

            committed_until = start_offset
            rows_since_maintenance = 0

            for offset in range(start_offset, total_rows, self.chunk_size):
                remaining = total_rows - offset
                rows_to_generate = min(self.chunk_size, remaining)
                print(
                    f"[{offset:,} → {offset+rows_to_generate:,}] Generating {rows_to_generate:,} rows…"
                )

                df = self.fn_generate(metadata, num_records=rows_to_generate)
                cols = list(df.columns)
                data_dict = {c: df[c].tolist() for c in cols}

                inserted = self.adapter.insert_rows_batch(
                    full_table_name=self.target_table,
                    data=data_dict,
                    columns=cols,
                    cursor=cur,
                    max_tuples_per_insert=max_tuples_per_insert,
                    max_sql_chars=max_sql_chars,
                )
                print(f"  ↳ inserted {inserted:,} rows")

                committed_until = offset + rows_to_generate
                rows_since_maintenance += rows_to_generate

                if rows_since_maintenance >= self.maintenance_every_rows:
                    print(f"Running maintenance at ~{committed_until:,} rows…")
                    self.post_maintenance()
                    self._save_offset(committed_until)
                    rows_since_maintenance = 0

            # final maintenance + checkpoint
            self.post_maintenance()
            self._save_offset(committed_until)

        finally:
            cur.close()

        print("=== Load complete ===")
