import contextlib
import re
from pathlib import Path, PurePath

from oasis_data_manager.filestore.backends.base import BaseStorage, MissingInputsException

WINDOWS_DRIVE_PATH = re.compile(r"/[A-Za-z]:/")


def path_to_file_url(path: PurePath) -> str:
    """Build the ``file://`` URL of an absolute local path.

    The path part of the URL always starts with ``/``: ``C:\\data\\loc.csv`` becomes
    ``file:///C:/data/loc.csv``, not ``file://C:\\data\\loc.csv``, in which ``C:\\data\\loc.csv``
    is the host and the path is empty. POSIX paths are unchanged (``file:///data/loc.csv``).
    Characters are not percent-encoded, as before.
    """
    posix_path = path.as_posix()
    if not posix_path.startswith("/"):
        posix_path = "/" + posix_path
    return f"file://{posix_path}"


def file_url_to_path(url: str) -> str:
    """Inverse of ``path_to_file_url``: the local path of a ``file://`` URL."""
    path = url[len("file://"):] if url.startswith("file://") else url
    if WINDOWS_DRIVE_PATH.match(path):
        path = path[1:]
    return path


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
        return filename, path_to_file_url(Path(self.root_dir, filename).absolute())

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
