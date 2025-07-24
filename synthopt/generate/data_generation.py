import pandas as pd
import numpy as np
import random
import random
import warnings
warnings.filterwarnings('ignore')
import ast
import string
from distfit import distfit
from scipy import stats
from tqdm import tqdm
from scipy.linalg import cholesky
from scipy.stats import norm
import datetime
import math

def generate_random_string():
    return "".join(
        random.choices("abcdefghijklmnopqrstuvwxyz", k=random.randint(5, 10))
    )


def generate_random_integer(value_range):
    if isinstance(value_range, (list)):
        return random.choice(value_range)
    elif isinstance(value_range, (tuple)):
        if value_range == (0, 1):  # if binary
            return random.choice([0, 1])
        else:
            return random.randint(value_range[0], value_range[1])
    else:
        return None


def generate_random_float(value_range):
    if isinstance(value_range, (list)):
        return random.choice(value_range)
    elif isinstance(value_range, (tuple)):
        return random.uniform(value_range[0], value_range[1])
    else:
        return None


def convert_datetime(metadata, generated_data):
    for column in generated_data.columns:
        if ("date" in metadata[metadata["variable_name"] == column]["datatype"].values[0]):
            datetime_format = metadata[metadata["variable_name"] == column]["coding"].values[0]
            generated_data[column] = pd.to_datetime(generated_data[column].astype("float"), unit="s").dt.strftime(datetime_format) #changes from as type Int64
    return generated_data


def decode_categorical_string(metadata, data):
    for _, row in metadata.iterrows():
        if row["datatype"] == "categorical string":
            variable = row["variable_name"]
            if not isinstance(row["coding"], dict):
                coding = ast.literal_eval(row["coding"])
            else:
                coding = row["coding"]
            if variable in data.columns:
                data[variable] = data[variable].map(coding)
    return data


def completeness(metadata, data):
    adjusted_data = data.copy()
    num_rows = len(data)
    for _, row in metadata.iterrows():
        col_name = row["variable_name"]
        completeness_level = row["completeness"]
        if col_name in adjusted_data.columns and not pd.isna(completeness_level):
            retain_count = int((completeness_level / 100) * num_rows)
            if retain_count < num_rows:
                retained_indices = np.random.choice(
                    adjusted_data.index, retain_count, replace=False
                )
                adjusted_data.loc[
                    ~adjusted_data.index.isin(retained_indices), col_name
                ] = np.nan
    return adjusted_data


def add_identifier(data, metadata, identifier_column, num_records):
    if identifier_column != None:
        if (
            "integer"
            in metadata[metadata["variable_name"] == identifier_column]["datatype"].values[0]
        ):
            participant_ids_integer = random.sample(
                range(1_000_000_000, 10_000_000_000), num_records
            )
            data[identifier_column] = participant_ids_integer
        elif (
            "float" in metadata[metadata["variable_name"] == identifier_column]["datatype"].values[0]
        ):
            participant_ids_float = [
                random.uniform(1_000_000_000, 10_000_000_000)
                for _ in range(num_records)
            ]
            data[identifier_column] = participant_ids_float
        else:
            participant_ids_string = [
                "".join(random.choices(string.ascii_letters + string.digits, k=10))
                for _ in range(num_records)
            ]
            data[identifier_column] = participant_ids_string
    return data


def add_shared_identifier(tables_dict, metadata, identifier_column, num_records):
    # Determine data type for the identifier column from any one table
    example_table_name = next(iter(tables_dict))
    example_metadata = metadata[metadata['table_name'] == example_table_name]
    identifier_dtype = example_metadata[example_metadata['variable_name'] == identifier_column]['datatype'].values[0]

    # Generate shared IDs based on the data type
    if "integer" in identifier_dtype:
        shared_ids = random.sample(range(1_000_000_000, 10_000_000_000), num_records)
    elif "float" in identifier_dtype:
        shared_ids = [random.uniform(1_000_000_000, 10_000_000_000) for _ in range(num_records)]
    else:
        shared_ids = [
            "".join(random.choices(string.ascii_letters + string.digits, k=10))
            for _ in range(num_records)
        ]

    # Assign the shared IDs to each table
    for table_name, df in tables_dict.items():
        if identifier_column in df.columns:
            df[identifier_column] = shared_ids

    return tables_dict


def generate_random_value(row):
    dtype = row["datatype"]
    value_range = row["values"]
    if "string" in str(dtype) and "categorical" not in str(dtype):
        return generate_random_string()
    else:
        try:
            if isinstance(value_range, str):
                value_range = eval(
                    value_range
                )  # Evaluate the string representation of a tuple/list
            if isinstance(value_range, (tuple, list)):
                if (
                    "int" in str(dtype)
                    or "date" in str(dtype)
                    or "categorical" in str(dtype)
                ):
                    return generate_random_integer(value_range)
                elif "float" in str(dtype):
                    return generate_random_float(value_range)
        except Exception as e:
            return None
        

def enforce_categorical_validity(table_metadata, df):
    for _, row in table_metadata.iterrows():
        column = row['variable_name']
        datatype = row['datatype']
        allowed_values = row.get('values')

        if 'categorical' in str(datatype).lower() and isinstance(allowed_values, list):
            valid_set = set(allowed_values)

            def closest_valid(val):
                if pd.isna(val) or val in valid_set:
                    return val
                try:
                    return min(allowed_values, key=lambda x: abs(x - val))
                except:
                    return val  # fallback

            df[column] = df[column].apply(closest_valid)

    return df




################################# STATISTICS GENERATION #################################



def generate_from_distributions(metadata, n_samples):
    synthetic_data = {}
    params_data = metadata[['dist', 'params']]
    dist_name = params_data['dist']
    params = params_data['params']

    # Generate data based on the distribution name and parameters
    if dist_name == 'norm':
        # Normal distribution (mean, std)
        synthetic_data = stats.norm.rvs(*params, size=n_samples)
    elif dist_name == 'expon':
        # Exponential distribution (loc, scale)
        synthetic_data = stats.expon.rvs(*params, size=n_samples)
    elif dist_name == 'uniform':
        # Uniform distribution (loc, scale)
        synthetic_data = stats.uniform.rvs(*params, size=n_samples)
    elif dist_name == 'gamma':
        # Gamma distribution (shape, loc, scale)
        synthetic_data = stats.gamma.rvs(*params, size=n_samples)
    elif dist_name == 'beta':
        # Beta distribution (alpha, beta, loc, scale)
        if len(params) == 4:
            synthetic_data = stats.beta.rvs(*params, size=n_samples)
        else:
            synthetic_data = stats.beta.rvs(*params[:2], size=n_samples)
    elif dist_name == 'lognorm':
        # Log-normal distribution (mean, std, loc)
        synthetic_data = stats.lognorm.rvs(*params, size=n_samples)
    elif dist_name == 'dweibull':
        # Support for the Weibull distribution
        synthetic_data = stats.dweibull.rvs(*params, size=n_samples)
    else:
        # If it's a different distribution, use scipy's distribution
        try:
            dist = getattr(stats, str(dist_name))
            synthetic_data = dist.rvs(*params, size=n_samples)
        except AttributeError:
            synthetic_data = generate_random_integer(metadata)

    return pd.DataFrame(synthetic_data)






################################# CORRELATION GENERATION #################################

def generate_from_correlations(column_metadata, num_records, correlation_matrix):
    # Ensure correlation_matrix is a numpy array
    correlation_matrix = correlation_matrix.to_numpy() if isinstance(correlation_matrix, pd.DataFrame) else correlation_matrix

    # Check for NaN or Inf values in the correlation matrix
    if np.isnan(correlation_matrix).any() or np.isinf(correlation_matrix).any():
        raise ValueError("Correlation matrix contains NaN or Inf values.")
    
    # Symmetrize the correlation matrix if it's not symmetric
    if not np.allclose(correlation_matrix, correlation_matrix.T):
        warnings.warn("Correlation matrix is not symmetric. Symmetrizing it.")
        correlation_matrix = (correlation_matrix + correlation_matrix.T) / 2

    # Check if the correlation matrix is positive definite
    if not np.all(np.linalg.eigvals(correlation_matrix) > 0):
        raise ValueError("Correlation matrix must be positive definite.")

    # Step 1: Generate multivariate normal samples
    mean = np.zeros(len(correlation_matrix))
    mvn_samples = np.random.multivariate_normal(mean, correlation_matrix, size=num_records)

    # Step 2: Transform to uniform marginals using the CDF of the normal distribution
    uniform_samples = norm.cdf(mvn_samples)

    # Step 3: Transform to original marginal distributions
    synthetic_data = {}
    for i, column in enumerate(column_metadata['variable_name']):
        dist_name = column_metadata.loc[column_metadata['variable_name'] == column, 'dist'].values[0]
        params = column_metadata.loc[column_metadata['variable_name'] == column, 'params'].values[0]

        # Get the distribution object from scipy.stats
        dist = getattr(stats, dist_name, None)
        if dist:
            synthetic_data[column] = dist.ppf(uniform_samples[:, i], *params)
        else:
            # Fallback to random integer generation if distribution is not found
            synthetic_data[column] = generate_random_integer(params)

    return pd.DataFrame(synthetic_data)


def convert_datetime_no_pandas(metadata, generated_data):
    """
    Convert float timestamps to formatted datetime strings for columns with 'date' in datatype.
    metadata: list of dicts
    generated_data: dict of lists (column: list of values)
    Returns: dict of lists (in-place modification)
    """
    meta_lookup = {row["variable_name"]: row for row in metadata}
    for column, values in generated_data.items():
        meta = meta_lookup.get(column, {})
        if "date" in str(meta.get("datatype", "")):
            fmt = meta.get("coding")
            if fmt:
                generated_data[column] = [
                    datetime.datetime.fromtimestamp(v).strftime(fmt) if v is not None and not (isinstance(v, float) and math.isnan(v)) else v
                    for v in values
                ]
    return generated_data


def decode_categorical_string_no_pandas(metadata, generated_data):
    """
    Decode categorical string columns using coding dict from metadata.
    metadata: list of dicts
    generated_data: dict of lists (column: list of values)
    Returns: dict of lists (in-place modification)
    """
    for row in metadata:
        if row.get("datatype") == "categorical string":
            variable = row["variable_name"]
            coding = row.get("coding")
            if coding and isinstance(coding, str):
                try:
                    coding = ast.literal_eval(coding)
                except Exception:
                    continue
            if coding and isinstance(coding, dict) and variable in generated_data:
                generated_data[variable] = [coding.get(v, v) for v in generated_data[variable]]
    return generated_data


def completeness_no_pandas(metadata, generated_data):
    """
    Randomly set values to None to match the completeness percentage in metadata.
    metadata: list of dicts
    generated_data: dict of lists (column: list of values)
    Returns: dict of lists (in-place modification)
    """
    num_rows = len(next(iter(generated_data.values()))) if generated_data else 0
    for row in metadata:
        col_name = row["variable_name"]
        completeness_level = row.get("completeness")
        if col_name in generated_data and completeness_level is not None:
            retain_count = int((completeness_level / 100) * num_rows)
            indices = list(range(num_rows))
            random.shuffle(indices)
            retain_indices = set(indices[:retain_count])
            generated_data[col_name] = [
                v if i in retain_indices else None
                for i, v in enumerate(generated_data[col_name])
            ]
    return generated_data


def add_shared_identifier_no_pandas(tables_dict, metadata, identifier_column, num_records):
    """
    Add a shared identifier column to all tables in tables_dict.
    tables_dict: dict of {table_name: {column: list of values}}
    metadata: list of dicts
    identifier_column: str
    num_records: int
    Returns: tables_dict (in-place modification)
    """
    # Find the datatype for the identifier column
    id_meta = next((row for row in metadata if row["variable_name"] == identifier_column), None)
    if not id_meta:
        return tables_dict
    dtype = id_meta.get("datatype", "")
    if "integer" in dtype:
        shared_ids = random.sample(range(1_000_000_000, 10_000_000_000), num_records)
    elif "float" in dtype:
        shared_ids = [random.uniform(1_000_000_000, 10_000_000_000) for _ in range(num_records)]
    else:
        shared_ids = [
            "".join(random.choices(string.ascii_letters + string.digits, k=10))
            for _ in range(num_records)
        ]
    for table in tables_dict.values():
        table[identifier_column] = shared_ids[:]
    return tables_dict


