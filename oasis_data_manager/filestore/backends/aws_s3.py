"""Backward-compatibility shim — import from the new location instead.

    from oasis_data_manager.filestore.backends.aws import AwsS3Storage
"""
import warnings
warnings.warn(
    "oasis_data_manager.filestore.backends.aws_s3 is deprecated. "
    "Use oasis_data_manager.filestore.backends.aws instead.",
    DeprecationWarning,
    stacklevel=2,
)

from oasis_data_manager.filestore.backends.aws import AwsS3Storage  # noqa: F401, E402

__all__ = ["AwsS3Storage"]
