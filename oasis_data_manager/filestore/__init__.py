from .backends.base import BaseStorage
from .backends.local import LocalStorage
from .config import get_storage_from_config, get_storage_from_config_path

__all__ = ["BaseStorage", "LocalStorage", "get_storage_from_config", "get_storage_from_config_path"]

try:
    from .backends.aws import AwsS3Storage
    __all__ += ["AwsS3Storage"]
except (ImportError, ModuleNotFoundError):
    pass

try:
    from .backends.azure import AzureABFSStorage
    __all__ += ["AzureABFSStorage"]
except (ImportError, ModuleNotFoundError):
    pass
