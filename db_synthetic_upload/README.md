# Database Synthetic Data Upload Module

This module provides tools and utilities for generating, processing, and uploading synthetic data to database systems, specifically designed to work with Trino and Iceberg tables. The module is structured to handle large-scale synthetic data generation with efficient chunked processing and database integration.


## Folder Structure

### `dataset_helpers/`
Contains dataset-specific helper modules for data extraction and processing:

- **`pedw.py`** - Patient Episode Database for Wales (PEDW) helpers
  - Healthcare data extraction utilities
  - Table schema inspection and column management

- **`wdsd.py`** - Welsh Demographic Service Dataset helpers
  - Demographic data processing utilities
  - Integration with WDSD database structures

###  `loading_datasets/`
Contains scripts for loading and processing synthetic datasets into database systems:

- **`structural_synthetic_pedw.py`** - PEDW structural synthetic data generation
  - Processes PEDW admissions data for synthetic generation
  - Implements chunked processing for large datasets
  - Progress tracking and resumable operations
  - Structural metadata preservation

- **`pedw_structural_synthetic.py`** - Alternative PEDW synthetic data processor
  - Additional implementation for PEDW data handling
  - Optimized for different use cases

- **`wdsd_structural_synthetic.py`** - WDSD synthetic data generation
  - Handles Welsh demographic data synthesis
  - Maintains demographic distribution patterns

###  `synthetic_alfs/`
Specialized modules for Anonymous Linking Field (ALF) synthetic data generation:

- **`synthetic_alfs.py`** - Core ALF synthetic data generator
  - Generates synthetic ALF values while preserving uniqueness
  - Multi-threaded processing for performance
  - Database integration with automatic table creation
  - Progress tracking and maintenance operations

- **`all_alfs.py`** - Comprehensive ALF processing system
  - Extracts and processes all distinct ALF values
  - Concurrent processing with thread safety
  - Automated database maintenance and optimization

###  `synthetic_load/`
Contains the core loading infrastructure for synthetic data:

- **`icebergloader.py`** - Reusable synthetic data loader for Trino+Iceberg
  - Generic loader class for any dataset type
  - Configurable chunk sizes and processing parameters
  - Progress tracking and resumability features
  - Pluggable functions for custom data processing
  - Automatic database maintenance and optimization


## Usage Examples

### Basic Synthetic Data Generation
```python
from synthetic_load.icebergloader import IcebergSyntheticLoader
from db_adapter import TrinoDBAdapter

# Initialize database adapter
adapter = TrinoDBAdapter(username="your_username", host="your_host")

# Create loader instance
loader = IcebergSyntheticLoader(
    adapter=adapter,
    target_table="iceberg.username.synthetic_data",
    chunk_size=250_000,
    progress_file="progress.txt"
)

# Run synthetic data generation
loader.run()
```

### Dataset-Specific Processing
```python
from dataset_helpers.pedw import get_table_admisions
from synthetic_alfs.synthetic_alfs import generate_synthetic_alfs

# Process PEDW data
admissions_data = get_table_admisions(adapter, limit=10000)

# Generate synthetic ALFs
generate_synthetic_alfs(target_count=1000000)
```

## Configuration

### Database Configuration
- Configure your Trino username in the respective scripts
- Ensure proper database permissions for table creation and data insertion
- Set appropriate host configurations for your Trino cluster

### Processing Parameters
- **Chunk Size**: Adjust based on available memory (100K-500K recommended)
- **Progress Files**: Customize paths for progress tracking
- **Date Formats**: Configure date parsing patterns as needed
- **Maintenance Intervals**: Set automatic maintenance frequency

## Dependencies

- `pandas` - Data manipulation and analysis
- `synthopt` - Core synthetic data generation library
- Custom `db_adapter` - Database connectivity utilities
- `concurrent.futures` - Multi-threading support
- `threading` - Thread safety mechanisms
