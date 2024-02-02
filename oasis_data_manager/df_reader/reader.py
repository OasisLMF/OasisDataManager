__all__ = [
    'OasisReader',
    'OasisPandasReader',
    'OasisPandasReaderCSV',
    'OasisPandasReaderParquet',
    'OasisDaskReader',
    'OasisDaskReaderCSV',
    'OasisDaskReaderParquet',
]

"""
    Readers to replace direct usage of pd.read_csv/read_parquet and allows for filters() & sql()
    to be provided.
"""

from .backends.base import OasisReader
from .backends.pandas import OasisPandasReader, OasisPandasReaderCSV, OasisPandasReaderParquet

try:
    from .backends.dask import OasisDaskReader, OasisDaskReaderCSV, OasisDaskReaderParquet
except ModuleNotFoundError as e:
    pass
