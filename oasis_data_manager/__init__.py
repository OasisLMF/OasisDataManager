__version__ = '0.2.1'

# --- Short reader aliases -------------------------------------------------- #
from .df_reader.backends.base import OasisReader
from .df_reader.backends.pandas import (
    OasisPandasReader as PandasReader,
    OasisPandasReaderCSV as PandasReaderCSV,
    OasisPandasReaderParquet as PandasReaderParquet,
)

__all__ = [
    "OasisReader",
    "PandasReader",
    "PandasReaderCSV",
    "PandasReaderParquet",
]

try:
    from .df_reader.backends.dask import (
        OasisDaskReader as DaskReader,
        OasisDaskReaderCSV as DaskReaderCSV,
        OasisDaskReaderParquet as DaskReaderParquet,
    )
    __all__ += ["DaskReader", "DaskReaderCSV", "DaskReaderParquet"]
except (ImportError, ModuleNotFoundError):
    pass

try:
    from .df_reader.backends.pyarrow import OasisPyarrowReader as PyarrowReader
    __all__ += ["PyarrowReader"]
except (ImportError, ModuleNotFoundError):
    pass

# --- Short storage aliases ------------------------------------------------- #
from .filestore.backends.base import BaseStorage
from .filestore.backends.local import LocalStorage
from .filestore.config import get_storage_from_config, get_storage_from_config_path

__all__ += ["BaseStorage", "LocalStorage", "get_storage_from_config", "get_storage_from_config_path"]

# --- Exceptions ------------------------------------------------------------ #
from .errors import OasisDataManagerException, OasisException

__all__ += ["OasisDataManagerException", "OasisException"]

try:
    from .filestore.backends.aws import AwsS3Storage
    __all__ += ["AwsS3Storage"]
except (ImportError, ModuleNotFoundError):
    pass

try:
    from .filestore.backends.azure import AzureABFSStorage
    __all__ += ["AzureABFSStorage"]
except (ImportError, ModuleNotFoundError):
    pass
