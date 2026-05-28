# OasisDataManager

A Python library providing unified data access patterns across different storage backends and DataFrame engines, as part of the [OasisLMF](https://github.com/OasisLMF) catastrophe modelling platform.

It abstracts:
- **File I/O** — local filesystem, AWS S3, Azure Blob Storage
- **DataFrame reading** — pandas, Dask, PyArrow
- **Data pipelines** — fetch → filter → SQL → transform, composable via a fluent API

---

## Installation

```bash
# Core install (pandas + local storage only)
pip install oasis-data-manager

# Development install
pip install -e .
pip install -r requirements.txt

# With optional cloud and distributed features (Dask, S3, Azure, PyArrow)
pip install -e ".[extra]"
```

---

## Quick start

```python
from oasis_data_manager.df_reader.backends.pandas import OasisPandasReader
from oasis_data_manager.filestore.backends.local import LocalStorage

storage = LocalStorage("/data")

# Read a CSV and get a pandas DataFrame
df = OasisPandasReader("accounts.csv", storage).as_pandas()

# Chain filters
df = (
    OasisPandasReader("accounts.csv", storage)
    .filter([lambda x: x[x["PortNumber"] == "1"]])
    .as_pandas()
)
```

---

## Storage backends

Three backends are provided. All share the same interface.

### Local

```python
from oasis_data_manager.filestore.backends.local import LocalStorage

storage = LocalStorage(root_dir="/data")
```

### AWS S3

```python
from oasis_data_manager.filestore.backends.aws import AwsS3Storage

storage = AwsS3Storage(
    bucket_name="my-bucket",
    access_key="AKIA...",
    secret_key="...",
    root_dir="models/",          # optional sub-path within the bucket
)
```

### Azure Blob Storage

```python
from oasis_data_manager.filestore.backends.azure import AzureABFSStorage

storage = AzureABFSStorage(
    account_name="myaccount",
    account_key="...",
    azure_container="my-container",
    root_dir="models/",          # optional sub-path within the container
)
```

### Configuration dict pattern

Used throughout the OasisLMF platform to configure storage from serialisable dicts:

```python
from oasis_data_manager.filestore.config import get_storage_from_config

config = {
    "storage_class": "AwsS3Storage",
    "options": {
        "bucket_name": "my-bucket",
        "access_key": "AKIA...",
        "secret_key": "...",
    }
}
storage = get_storage_from_config(config)
```

### Common storage operations

```python
# Open a file (context manager, like built-in open)
with storage.open("path/to/file.csv") as f:
    data = f.read()

# Copy a file to a local temp directory
local_path = storage.get("remote/file.parquet", "/tmp/")

# Upload a file
storage.put("/tmp/output.csv", "remote/output.csv")

# Delete
storage.delete_file("remote/old.csv")
```

---

## DataFrame readers

### Reader backends

| Class | Engine | Formats | Filter behaviour |
|---|---|---|---|
| `OasisPandasReader` | pandas | CSV, Parquet | In-memory (post-load) |
| `OasisDaskReader` | Dask | CSV, Parquet | In-memory via dask-sql |
| `OasisPyarrowReader` | PyArrow | Parquet only | Predicate pushdown (pre-load) |

Format-specific subclasses (`OasisPandasReaderCSV`, `OasisDaskReaderParquet`, etc.) are also available.

### Fluent API

All readers share the same chainable interface. The actual file read is **lazy** — it happens on the first access to `.df` or when `.as_pandas()` is called.

```python
from oasis_data_manager.df_reader.backends.pandas import OasisPandasReader
from oasis_data_manager.df_reader.backends.dask import OasisDaskReader
from oasis_data_manager.filestore.backends.local import LocalStorage

storage = LocalStorage("/data")

# Pandas — CSV
df = OasisPandasReader("losses.csv", storage).as_pandas()

# Pandas — Parquet (detected automatically from extension)
df = OasisPandasReader("losses.parquet", storage).as_pandas()

# Dask
df = OasisDaskReader("losses.csv", storage).as_pandas()
```

### Filtering

Pass a list of callables; each receives the DataFrame and must return a (filtered) DataFrame.

```python
df = (
    OasisPandasReader("locations.csv", storage)
    .filter([
        lambda x: x[x["CountryCode"] == "US"],
        lambda x: x[x["LocNumber"].notna()],
    ])
    .as_pandas()
)
```

`OasisPandasReader` and `OasisDaskReader` apply filters **after** loading the full file into memory. `OasisPyarrowReader` accepts a `filters` kwarg (list of tuples or list of lists) that is pushed down into the Parquet engine before any data is read into memory — use this for large Parquet files where row-group skipping matters.

```python
from oasis_data_manager.df_reader.backends.pyarrow import OasisPyarrowReader

# AND of conditions — list of tuples
df = (
    OasisPyarrowReader("losses.parquet", storage)
    .read(filters=[("CountryCode", "==", "US"), ("TIV", ">=", 1_000_000)])
    .as_pandas()
)

# OR of AND-groups — list of lists
df = (
    OasisPyarrowReader("losses.parquet", storage)
    .read(filters=[[("CountryCode", "==", "US")], [("CountryCode", "==", "GB")]])
    .as_pandas()
)
```

Supported operators: `==`, `!=`, `<`, `<=`, `>`, `>=`, `in`, `not in`.

### SQL (Dask only)

Requires `dask-sql`. The reserved table name is `table`.

```python
from oasis_data_manager.df_reader.backends.dask import OasisDaskReader

df = (
    OasisDaskReader("locations.csv", storage)
    .sql("SELECT LocNumber, Latitude, Longitude FROM table WHERE CountryCode = 'US'")
    .as_pandas()
)
```

### Arbitrary queries

`.query(fn)` passes the raw DataFrame to any callable and returns the result directly (not a reader).

```python
count = OasisPandasReader("losses.csv", storage).query(lambda df: len(df))
```

### Configuration dict pattern

```python
from oasis_data_manager.df_reader.config import get_df_reader

config = {
    "path": "accounts.csv",
    "storage": storage,
    "options": {"dtype": {"LocNumber": str}},
    "engine": "OasisPandasReaderCSV",
}
reader = get_df_reader(config)
df = reader.as_pandas()
```

---

## Complex data pipelines

`ComplexData` composes storage fetch, SQL filtering, and post-read transformations into a single reusable class.

### `FileStoreComplexData` — files not handled by the df_reader (e.g. HDF5)

```python
from oasis_data_manager.complex import FileStoreComplexData, Adjustment
import h5py
import pandas as pd

class NormaliseAdjustment(Adjustment):
    @classmethod
    def apply(cls, df):
        df["loss"] = df["loss"] / df["loss"].max()
        return df

class EventLossData(FileStoreComplexData):
    filename = "event_losses.hdf5"
    sql = "SELECT * FROM table WHERE event_id > 1000"
    adjustments = [NormaliseAdjustment]

    def to_dataframe(self, result) -> pd.DataFrame:
        f = h5py.File(result)
        return pd.DataFrame({"event_id": list(f["event_id"]), "loss": list(f["loss"])})

# Run the pipeline
df = EventLossData(storage=storage).run().as_pandas()
```

### `RestComplexData` — HTTP endpoints

```python
from oasis_data_manager.complex import RestComplexData

class ExposureAPI(RestComplexData):
    url = "https://api.example.com/exposures"
    timeout = 30

    def get_headers(self):
        return {"Authorization": "Bearer my-token"}

    def handle_response(self, response):
        return response.json()["data"]

df = ExposureAPI().run().as_pandas()
```

---

## Exceptions

| Exception | Description |
|---|---|
| `OasisDataManagerException` | Base exception for this library |
| `OasisException` | Backward-compatible alias for the above |
| `MissingInputsException` | Raised when a required input file is not found |

```python
from oasis_data_manager.errors import OasisDataManagerException, MissingInputsException
```

---

## Import paths

```python
# Storage backends
from oasis_data_manager.filestore.backends.local import LocalStorage
from oasis_data_manager.filestore.backends.aws import AwsS3Storage
from oasis_data_manager.filestore.backends.azure import AzureABFSStorage
from oasis_data_manager.filestore.config import get_storage_from_config

# DataFrame readers
from oasis_data_manager.df_reader.backends.pandas import OasisPandasReader, OasisPandasReaderCSV, OasisPandasReaderParquet
from oasis_data_manager.df_reader.backends.dask import OasisDaskReader, OasisDaskReaderCSV, OasisDaskReaderParquet
from oasis_data_manager.df_reader.backends.pyarrow import OasisPyarrowReader
from oasis_data_manager.df_reader.config import get_df_reader

# Exceptions
from oasis_data_manager.errors import OasisDataManagerException, OasisException
```

Deprecated module paths (`filestore/backends/aws_s3.py`, `filestore/backends/azure_abfs.py`) still work but emit a `DeprecationWarning`.

---

## Development

```bash
# Install dev dependencies
pip install -e .
pip install -r requirements.txt

# Run tests
pytest

# Skip type checking and import sorting for faster iteration
pytest --no-header -p no:mypy -p no:isort tests/df_reader/

# Cloud integration tests (requires Docker)
docker compose up -d
pytest tests/filestorage/test_aws.py tests/filestorage/test_azure.py
docker compose down

# Linting
flake8 --select F401,F522,F524,F541 --show-source ./
autopep8 --diff --exit-code --recursive --max-line-length 150 --ignore E402 .

# Build
python setup.py sdist && python setup.py bdist_wheel
```

---

## License

Part of the [OasisLMF](https://github.com/OasisLMF) platform. See repository for licence details.
