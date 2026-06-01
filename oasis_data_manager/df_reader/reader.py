"""
    Readers to replace direct usage of pd.read_csv/read_parquet and allows for filters() & sql()
    to be provided.
"""

from .backends.base import OasisReader
from .backends.pandas import OasisPandasReader, OasisPandasReaderCSV, OasisPandasReaderParquet

__all__ = [
    'OasisReader',
    'OasisPandasReader',
    'OasisPandasReaderCSV',
    'OasisPandasReaderParquet',
]

try:
    from .backends.pyarrow import OasisPyarrowReader
    __all__ += ['OasisPyarrowReader']
except ModuleNotFoundError:
    pass

try:
    from .backends.dask import OasisDaskReader, OasisDaskReaderCSV, OasisDaskReaderParquet
    __all__ += ['OasisDaskReader', 'OasisDaskReaderCSV', 'OasisDaskReaderParquet']
except ModuleNotFoundError:
    pass
