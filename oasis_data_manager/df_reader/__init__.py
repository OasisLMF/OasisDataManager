from .backends.base import OasisReader
from .backends.pandas import OasisPandasReader, OasisPandasReaderCSV, OasisPandasReaderParquet
from .config import get_df_reader, clean_config

__all__ = [
    "OasisReader",
    "OasisPandasReader",
    "OasisPandasReaderCSV",
    "OasisPandasReaderParquet",
    "get_df_reader",
    "clean_config",
]

try:
    from .backends.dask import OasisDaskReader, OasisDaskReaderCSV, OasisDaskReaderParquet
    __all__ += ["OasisDaskReader", "OasisDaskReaderCSV", "OasisDaskReaderParquet"]
except (ImportError, ModuleNotFoundError):
    pass

try:
    from .backends.pyarrow import OasisPyarrowReader
    __all__ += ["OasisPyarrowReader"]
except (ImportError, ModuleNotFoundError):
    pass
