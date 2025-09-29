import os
import pandas as pd
from dataset_helpers.wdsd import WDSD_TABLES
from synthetic_load.icebergloader import IcebergSyntheticLoader
from synthopt.process.structural_metadata import process_structural_metadata
from synthopt.generate.structural_synthetic_data import (
    generate_structural_synthetic_data,
)
from synthopt.process.db_adapter import TrinoDBAdapter
from synthopt.process.db_helper import DBHelper


TARGET_SCHEMA = "iceberg.arthur"
SYNTHETIC_ALFS_TABLE = "iceberg.arthur.synthetic_alfs"


class WDSDSyntheticGenerator:
    def __init__(self, adapter: TrinoDBAdapter):
        self.adapter = adapter
        self.db_helper = DBHelper(adapter)


    def generate_synthetic_table(self, source_table: str, target_table: str):
        """Generate synthetic data for a single WDSD table"""
        print(f"\nGenerating synthetic data for {target_table}")

        # Set up IcebergLoader for this table
        loader = IcebergSyntheticLoader(
            adapter=self.adapter,
            target_table=target_table,
            chunk_size=500000,
            maintenance_every_rows=500000,
            progress_file=f"loader_progress_{target_table}.txt",
            fn_rowcount=lambda adapter: self.db_helper.get_table_rowcount(source_table),
            fn_sample=lambda adapter, limit, offset: self.db_helper.get_table_sample(
                source_table, limit, offset
            ),
            fn_columns=lambda adapter: self.db_helper.get_table_columns(source_table),
            fn_process_metadata=lambda df, datetime_formats: process_structural_metadata(
                df, datetime_formats=datetime_formats
            ),
            fn_generate=lambda metadata, num_records: self._generate_with_synthetic_alfs(
                metadata, num_records, source_table
            ),
        )

        # Collect metadata from original table
        metadata = loader.collect_metadata_sample(sample_size=100_000)

        # Create target table using the metadata and generated data types
        loader.ensure_target_table(metadata=metadata)

        # Generate and load synthetic data
        loader.load(metadata)

    def _generate_with_synthetic_alfs(
        self, metadata, num_records: int, source_table: str
    ) -> pd.DataFrame:
        """Generate synthetic data with ALF_E from synthetic_alfs table"""
        # First generate synthetic data for all columns
        df = generate_structural_synthetic_data(metadata, num_records)

        if "alf_e" in df.columns:
            # Get synthetic ALFs
            synthetic_alfs = self.db_helper.get_random_synthetic_alfs(
                SYNTHETIC_ALFS_TABLE,
                num_records,
                # Allow duplicates if original table has more rows than distinct ALFs
                allow_duplicates=self.db_helper.needs_duplicate_alfs(source_table),
            )
            # Replace generated ALFs with synthetic ones
            df["alf_e"] = synthetic_alfs

        return df


def main():
    """Main function to generate synthetic versions of all WDSD tables"""
    adapter = TrinoDBAdapter(
        username="zinnurar", host="trino.feasibility.sail.pk.serp.ac.uk"
    )
    generator = WDSDSyntheticGenerator(adapter)

    for table_name, source_table in WDSD_TABLES.items():
        target_table = f"{TARGET_SCHEMA}.synthetic_{table_name}"
        try:
            generator.generate_synthetic_table(source_table, target_table)
            print(f"Successfully generated {target_table}")
        except Exception as e:
            print(f"Error generating {target_table}: {e}")


if __name__ == "__main__":
    main()
