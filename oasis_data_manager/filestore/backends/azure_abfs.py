"""Backward-compatibility shim — import from the new location instead.

    from oasis_data_manager.filestore.backends.azure import AzureABFSStorage
"""
import warnings
warnings.warn(
    "oasis_data_manager.filestore.backends.azure_abfs is deprecated. "
    "Use oasis_data_manager.filestore.backends.azure instead.",
    DeprecationWarning,
    stacklevel=2,
)

from oasis_data_manager.filestore.backends.azure import AzureABFSStorage  # noqa: F401, E402

__all__ = ["AzureABFSStorage"]
