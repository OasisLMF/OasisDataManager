import contextlib
from pathlib import Path

from oasis_data_manager.filestore.backends.base import BaseStorage, MissingInputsException


class LocalStorage(BaseStorage):
    """
    Implements storage for a local filesystem. All paths passed to the
    storage should be relative to the media root.
    """

    storage_connector = "FS-SHARE"
    fsspec_filesystem_class = None

    @property
    def config_options(self):
        return {
            "root_dir": self.root_dir,
        }

    def get_storage_url(self, filename=None, suffix="tar.gz", **kwargs):
        filename = (
            filename if filename is not None else self._get_unique_filename(suffix)
        )
        return filename, f"file://{Path(self.root_dir, filename)}"

    def get_fsspec_storage_options(self):
        return {}

    @contextlib.contextmanager
    def with_fileno(self, path, mode="rb"):
        with self.open(path, mode) as f:
            yield f

    def get_from_cache(self, reference, required=False, no_cache_target=None):
        # null ref given
        if not reference:
            if required:
                raise MissingInputsException(reference)
            else:
                return None

        return self.fs._join(reference)
