from synthopt.process.structural_metadata import process_structural_metadata, process_structural_metadata_sql
from synthopt.generate.structural_synthetic_data import generate_structural_synthetic_data, generate_structural_synthetic_data_from_sql
from pedw import get_table_admisions
from db_adapter import TrinoDBAdapter
import pandas as pd

# Set date_formats to None as in new_test.py
# You can adjust this if you want to specify formats

date_formats = None

# Instantiate the adapter (update username/host as needed)
adapter = TrinoDBAdapter(username="zinnurar", host="trino.feasibility.sail.pk.serp.ac.uk")

# Fetch the admissions table as a list of tuples
rows = get_table_admisions(adapter, limit=10000)


# Get column names from the cursor description
cursor = adapter.get_cursor()
cursor.execute("SELECT * FROM iceberg.pedw.pedw_admissions_20231127")
columns = [desc[0] for desc in cursor.description]
cursor.close()



# Convert to DataFrame
DATA = pd.DataFrame(rows,columns=columns,dtype=str)
#print(DATA)

STRUCT_METADATA = process_structural_metadata(DATA, datetime_formats=date_formats)
#print(STRUCT_METADATA)


STRUCT_SYNTHETIC_DATA = generate_structural_synthetic_data(STRUCT_METADATA, num_records=len(rows))

#print(STRUCT_SYNTHETIC_DATA)

adapter.save_synthetic_table('pedw_admissions_20231127_structural_synthetic', STRUCT_SYNTHETIC_DATA)