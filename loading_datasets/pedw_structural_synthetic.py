from synthetic_load.icebergloader import IcebergSyntheticLoader
from synthopt.process.db_adapter import TrinoDBAdapter
from dataset_helpers.pedw import get_admissions_row_count, get_table_admisions, get_column_names
from synthopt.process.structural_metadata import process_structural_metadata
from synthopt.generate.structural_synthetic_data import generate_structural_synthetic_data


adapter = TrinoDBAdapter(username="zinnurar", host="trino.feasibility.sail.pk.serp.ac.uk")

loader = IcebergSyntheticLoader(
    adapter=adapter,
    target_table="iceberg.arthur.pedw_admissions_20231127_structural_synthetic",
    schema_qualified="iceberg.arthur",
    chunk_size=10000,                    #Chunk size for each insert to trino
    maintenance_every_rows=10000,          # run metadata hygiene every 500k
    progress_file="pedw_progress.txt",
    fn_rowcount=get_admissions_row_count,
    fn_sample=get_table_admisions,
    fn_columns=get_column_names,
    fn_process_metadata=process_structural_metadata,
    fn_generate=generate_structural_synthetic_data,
)

def main():
    metadata = loader.collect_metadata_sample(sample_size=100_000)
    loader.load(metadata, max_sql_chars=900_000, max_tuples_per_insert=250_000)

if __name__ == "__main__":
    main()
    
