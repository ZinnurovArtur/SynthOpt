from synthetic_load.icebergloader import IcebergSyntheticLoader
from synthopt.process.db_adapter import TrinoDBAdapter
from dataset_helpers.pedw import (
    get_admissions_row_count,
    get_table_admisions,
    get_column_names,
)
from synthopt.process.structural_metadata import process_structural_metadata
from synthopt.generate.structural_synthetic_data import (
    generate_structural_synthetic_data,
)
from synthopt.process.db_helper import DBHelper

USERNAME = "username"  # Replace with your Trino username
TARGET_SCHEMA = f"iceberg.{USERNAME}"
SYNTHETIC_ALFS_TABLE = f"iceberg.{USERNAME}.synthetic_alfs"


import numpy as np


def generate_pedw_with_synthetic_alfs(
    metadata, num_records, db_helper, source_table, proportion_from_table=0.8
):
    """Generate synthetic data for PEDW, mixing synthetic_alfs and random alf_e values."""
    df = generate_structural_synthetic_data(metadata, num_records)
    if "alf_e" in df.columns:
        n_from_table = int(num_records * proportion_from_table)
        # Randomly select indices to replace with referential alf_e
        indices = np.arange(num_records)
        np.random.shuffle(indices)
        referential_indices = indices[:n_from_table]
        # Get alf_e from table
        alf_e_from_table = db_helper.get_random_synthetic_alfs(
            SYNTHETIC_ALFS_TABLE,
            n_from_table,
            allow_duplicates=db_helper.needs_duplicate_alfs(source_table),
        )
        # Replace selected indices with referential alf_e
        df_alf_e = df["alf_e"].to_numpy()
        df_alf_e[referential_indices] = alf_e_from_table
        # Shuffle alf_e column to avoid any ordering bias
        np.random.shuffle(df_alf_e)
        df["alf_e"] = df_alf_e
    return df


def main():
    adapter = TrinoDBAdapter(
        username=USERNAME, host="trino.feasibility.sail.pk.serp.ac.uk"
    )
    db_helper = DBHelper(adapter)
    source_table = "iceberg.pedw.pedw_admissions_20250505"
    loader = IcebergSyntheticLoader(
        adapter=adapter,
        target_table=f"iceberg.{USERNAME}.pedw_admissions_20250505_structural_synthetic",
        schema_qualified=TARGET_SCHEMA,
        chunk_size=500000,
        maintenance_every_rows=500000,
        progress_file="pedw_progress.txt",
        fn_rowcount=get_admissions_row_count,
        fn_sample=get_table_admisions,
        fn_columns=get_column_names,
        fn_process_metadata=process_structural_metadata,
        fn_generate=lambda metadata, num_records: generate_pedw_with_synthetic_alfs(
            metadata, num_records, db_helper, source_table, proportion_from_table=0.8
        ),
    )
    metadata = loader.collect_metadata_sample(sample_size=100_000)
    loader.ensure_target_table(metadata=metadata)
    loader.load(metadata, max_sql_chars=900_000, max_tuples_per_insert=250_000)


if __name__ == "__main__":
    main()
