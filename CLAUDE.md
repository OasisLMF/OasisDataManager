# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OasisDataManager (`oasis-data-manager` on PyPI) is a Python data management library for the Oasis Loss Modelling Framework. It provides storage backend abstractions (local, AWS S3, Azure Blob), DataFrame reader abstractions (Pandas, Dask, PyArrow), and a complex data pipeline layer that combines fetching, reading, filtering, and adjusting data.

## Build & Development Commands

```bash
# Install dependencies
pip install pip-tools
pip install -r requirements.txt

# Run all tests (requires Docker services)
docker compose up -d   # Start LocalStack (S3) and Azurite (Azure Blob)
pytest

# Run a single test file
pytest tests/filestorage/test_local.py

# Run a single test
pytest tests/filestorage/test_local.py::TestLocalStorage::test_put_get -v

# Linting
flake8 oasis_data_manager/

# Type checking
mypy oasis_data_manager/
```

## Architecture

Three main modules, each following a base-class/backend pattern:

### FileStore (`oasis_data_manager/filestore/`)
Storage abstraction layer built on `fsspec`. `BaseStorage` defines the interface (`get`, `put`, `exists`, `extract`, `compress`, caching). Backends: `LocalStorage`, `AwsS3Storage`, `AzureABFSStorage`. Configuration via `StorageConfig` TypedDict with `storage_class` (dotted path string) and `options` dict. Factory: `get_storage_from_config()`.

### DataFrame Reader (`oasis_data_manager/df_reader/`)
Read CSV/Parquet files with optional filtering and SQL. `OasisReader` base class with `read_csv`, `read_parquet`, `filter`, `sql`, `as_pandas`. Backends: `OasisPandasReader`, `OasisDaskReader` (adds SQL via dask-sql), `OasisPyarrowReader`. Auto-detects format from file extension. Factory: `get_df_reader()`.

### Complex Data (`oasis_data_manager/complex/`)
High-level pipeline combining fetch, read, filter, and adjust. `ComplexData` orchestrates the flow: `fetch()` retrieves remote data, `get_df_reader()` wraps it in an OasisReader, `run()` executes the pipeline. `Adjustment` is a base class for pandas DataFrame transformations applied in sequence.

### Shared Utilities
- `config.py`: `load_class(path)` dynamically imports classes from dotted path strings.
- `errors/`: `OasisException` base exception.

## Code Style

- Max line length: 150 (flake8)
- flake8 ignores: E501, E402
- mypy: `follow_imports = skip`, `ignore_missing_imports = true`
- isort is in dependencies but not actively enforced

## Testing

- Tests mirror source structure: `tests/filestorage/`, `tests/df_reader/`, `tests/complex/`
- Storage tests use Docker services: LocalStack (port 4566) for S3, Azurite (port 10000) for Azure
- `tests/filestorage/test_general.py` uses hypothesis for property-based testing across all backends
- Storage test fixtures are context managers (`aws_s3_storage()`, `azure_abfs_storage()`, `local_storage()`)
- HTTP mocking uses `respx`

## Version

Version is stored in `oasis_data_manager/__init__.py` as `__version__`. The CI `version.yml` workflow updates it automatically.

## Branch & PR Conventions

- Main branch: `develop`
- Release branches: `release/x.y.z`
- PRs require 2+ reviewers and must link to an issue
