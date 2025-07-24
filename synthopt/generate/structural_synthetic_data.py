from synthopt.generate.data_generation import generate_random_value
from synthopt.generate.data_generation import convert_datetime_no_pandas, decode_categorical_string_no_pandas, completeness_no_pandas, add_shared_identifier_no_pandas
from tqdm import tqdm
import pandas as pd

def generate_structural_synthetic_data(metadata, num_records=1000, identifier_column=None):
    metadata = metadata.copy()

    # Initialize a dictionary to hold generated data for each table
    generated_data = {}

    # Create a mapping for each table to handle variable generation
    table_variable_mapping = {}

    for index, row in metadata.iterrows():
        table_name = row["table_name"]
        variable_name = row["variable_name"]

        # Initialize the table if it doesn't exist
        if table_name not in table_variable_mapping:
            table_variable_mapping[table_name] = []

        # Append variable row details to the specific table
        table_variable_mapping[table_name].append(row)

    # Loop through each table and generate its data
    for table_name, variables in table_variable_mapping.items():
        generated_data[table_name] = {}

        for row in tqdm(variables, desc="Generating Synthetic Data"):
            column_name = row["variable_name"]
            data = []

            # Generate data for the current variable
            for _ in range(num_records):
                value = generate_random_value(row)
                data.append(value)

            generated_data[table_name][column_name] = data

        # Create DataFrame for the current table and do conversions
        generated_data[table_name] = pd.DataFrame(generated_data[table_name])
        generated_data[table_name] = convert_datetime(metadata, generated_data[table_name])
        generated_data[table_name] = decode_categorical_string(
            metadata, generated_data[table_name]
        )
        generated_data[table_name] = completeness(metadata, generated_data[table_name])

    # Apply shared identifiers after all tables are generated
    if identifier_column is not None:
        generated_data = add_shared_identifier(
            generated_data, metadata, identifier_column, num_records
        )

    if "None" in generated_data:
        generated_data = generated_data["None"]

    return generated_data

def generate_structural_synthetic_data_from_sql(metadata, num_records=1000, identifier_column=None):
    """
    Generate structural synthetic data from raw SQL metadata (list of dicts, no pandas).
    Applies pure Python helpers for datetime conversion, categorical decoding, completeness, and shared identifier.
    Args:
        metadata (list of dict): Metadata for each variable/column (from process_structural_metadata_no_pandas).
        num_records (int): Number of synthetic records to generate per table.
        identifier_column: If provided, adds a shared identifier column.
    Returns:
        Dict of dicts: {table_name: {column: list of values}}
    """
    generated_data = {}
    table_variable_mapping = {}

    # Build mapping of table_name -> list of variable dicts
    for row in metadata:
        table_name = row.get("table_name", "None")
        if table_name not in table_variable_mapping:
            table_variable_mapping[table_name] = []
        table_variable_mapping[table_name].append(row)

    # Generate data for each table
    for table_name, variables in table_variable_mapping.items():
        generated_data[table_name] = {}

        for row in variables:
            column_name = row["variable_name"]
            data = []
            for _ in range(num_records):
                value = generate_random_value(row)
                data.append(value)
            generated_data[table_name][column_name] = data

        # Apply pure Python helpers
        generated_data[table_name] = convert_datetime_no_pandas(variables, generated_data[table_name])
        generated_data[table_name] = decode_categorical_string_no_pandas(variables, generated_data[table_name])
        generated_data[table_name] = completeness_no_pandas(variables, generated_data[table_name])

        # Count and print how many rows have been written for this table
        if generated_data[table_name]:
            first_col = next(iter(generated_data[table_name].values()))
            print(f"Table '{table_name}': {len(first_col)} rows written.")

    # Apply shared identifier if requested
    if identifier_column is not None:
        generated_data = add_shared_identifier_no_pandas(generated_data, metadata, identifier_column, num_records)

    # If only one table (table_name == 'None'), return just that table's data
    if "None" in generated_data:
        generated_data = generated_data["None"]

    return generated_data
