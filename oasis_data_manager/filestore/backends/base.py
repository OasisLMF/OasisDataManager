import base64
import contextlib
import io
import logging
import os
import shutil
import tarfile
import tempfile
import uuid
from pathlib import Path
from typing import Optional, Tuple, Type, Union
from urllib.parse import urlparse
from urllib.request import urlopen

import fsspec
from fsspec.implementations.dirfs import DirFileSystem

from oasis_data_manager.errors import OasisException

LOG_FILE_SUFFIX = "txt"
ARCHIVE_FILE_SUFFIX = "tar.gz"


class MissingInputsException(OasisException):
    def __init__(self, input_filepath):
        super(MissingInputsException, self).__init__(
            "Input file not found: {}".format(input_filepath)
        )


class StrictRootDirFs(DirFileSystem):
    def _path_is_in_root(self, path):
        return os.path.abspath(path).startswith(os.path.abspath(self.path))

    def _join(self, path):
        res = super()._join(path)

        if isinstance(res, str):
            if not self._path_is_in_root(res):
                raise FileNotFoundError(path)
        else:
            for p in res:
                if not self._path_is_in_root(p):
                    raise FileNotFoundError(p)

        return res

    def exists(self, path):
        try:
            return super().exists(path)
        except FileNotFoundError:
            return False

    def isfile(self, path):
        try:
            return super().isfile(path)
        except FileNotFoundError:
            return False

    def isdir(self, path):
        try:
            return super().isdir(path)
        except FileNotFoundError:
            return False


class BaseStorage(object):
    """Base storage class

    Implements storage for a local fileshare between
    `server` and `worker` containers
    """

    storage_connector: str
    fsspec_filesystem_class: Optional[Type[fsspec.AbstractFileSystem]]

    def __init__(
        self, root_dir="", cache_dir: Union[str, None] = "/tmp/data-cache", logger=None
    ):
        # Use for caching files across multiple runs, set value 'None' or 'False' to disable
        self.cache_root = cache_dir
        self.root_dir = root_dir

        self.logger = logger or logging.getLogger()
        self._fs: Optional[StrictRootDirFs] = None

    def to_config(self) -> dict:
        return {
            "storage_class": f"{self.__module__}.{type(self).__name__}",
            "options": self.config_options,
        }

    @property
    def config_options(self):
        raise NotImplementedError()

    def _get_unique_filename(self, suffix=""):
        """Returns a unique name

        Parameters
        ----------
        :param suffix: Set the filename extension
        :type suffix: str

        :return: filename string
        :rtype str
        """
        if suffix.startswith('.'):
            suffix = suffix[1:]
        return "{}.{}".format(uuid.uuid4().hex, suffix)

    def _is_valid_url(self, url):
        """Check if a String is a valid url

        Parameters
        ----------
        :param url: String to check
        :type  url: str

        :return: `True` if URL otherwise `False`
        :rtype boolean
        """
        if url:
            result = urlparse(url)
            return all([result.scheme, result.netloc]) and result.scheme in [
                "http",
                "https",
            ]
        else:
            return False

    def extract(self, archive_fp, directory, storage_subdir=""):
        """Extract tar file

        Parameters
        ----------
        :param archive_fp: Path to archive file
        :type  archive_fp: str

        :param directory: Path to extract contents to.
        :type  directory: str

        :param storage_subdir: Store object in given sub directory
        :type  storage_subdir: str
        """
        with tempfile.TemporaryDirectory() as temp_dir_path:
            local_archive_path = self.get(
                archive_fp,
                os.path.join(temp_dir_path, os.path.basename(archive_fp)),
                subdir=storage_subdir,
            )
            with tarfile.open(local_archive_path) as f:
                os.makedirs(directory, exist_ok=True)
                f.extractall(directory)

    def compress(self, archive_fp, directory, arcname=None):
        """Compress a directory

        Parameters
        ----------
        :param archive_fp: Path to archive file
        :type  archive_fp: str

        :param directory: Path to archive.
        :type  directory: str

        :param arcname: If given, `arcname' set an alternative
                        name for the file in the archive.
        :type arcname: str
        """
        arcname = arcname if arcname else "/"
        with tarfile.open(archive_fp, "w:gz") as tar:
            tar.add(directory, arcname=arcname)

    def get_from_cache(self, reference, required=False, no_cache_target=None):
        """
        Retrieves a file from the storage and stores it in the cache.
        If it already exists in te cache the fetching step will be skipped

        If URL: download the object and place in `output_dir`
        If Filename: return stored file path of the shared object

        Parameters
        ----------
        :param reference: Filename or download URL
        :type  reference: str

        :param no_cache_target: A path to store the file at if no cache root is set
        :type  no_cache_target: str

        :return: Absolute filepath to stored Object
        :rtype str
        """
        # null ref given
        if not reference:
            if required:
                raise MissingInputsException(reference)
            else:
                return None

        # check if the file is in the cache, if so return that path
        cache_filename = base64.b64encode(reference.encode()).decode()
        if self.cache_root:
            cached_file = os.path.join(self.cache_root, cache_filename)
        else:
            os.makedirs(os.path.dirname(no_cache_target), exist_ok=True)
            cached_file = no_cache_target

        if self.cache_root:
            if os.path.exists(cached_file):
                logging.info("Get from Cache: {}".format(reference))
                return cached_file

        if self._is_valid_url(reference):
            # if the file is not in the path, and is a url download it to the cache
            response = urlopen(reference)
            fdata = response.read()

            with io.open(cached_file, "w+b") as f:
                f.write(fdata)
                logging.info("Get from URL: {}".format(reference))
        else:
            # otherwise get it from the storage and add it to the cache
            self.fs.get(reference, cached_file, recursive=True)

        return cached_file

    def get(self, reference, output_path="", subdir="", required=False):
        """Retrieve stored object and stores it in the output path

        Top level 'get from storage' function
        Check if `reference` is either download `URL` or filename

        If URL: download the object and place in `output_dir`
        If Filename: return stored file path of the shared object

        Parameters
        ----------
        :param reference: Filename or download URL
        :type  reference: str

        :param output_path: If given, download to that directory.
        :type  output_path: str

        :param subdir: Store a file under this sub directory path
        :type  subdir: str

        :return: Absolute filepath to stored Object
        :rtype str
        """
        # null ref given
        if not reference:
            if required:
                raise MissingInputsException(reference)
            else:
                return None

        target = os.path.abspath(
            os.path.join(output_path, subdir) if subdir else output_path
        )

        if os.path.isdir(target):
            fname = reference
            if self._is_valid_url(reference):
                fname = os.path.basename(urlparse(reference).path)

            target = os.path.join(output_path, fname)

        res = self.get_from_cache(reference, required=required, no_cache_target=target)
        if res:
            if res != target:
                os.makedirs(os.path.dirname(target), exist_ok=True)
                shutil.copy(res, target)
            return target

        return None

    def put(self, reference, filename=None, subdir="", suffix=None, arcname=None):
        """Place object in storage

        Top level send to storage function,
        Create new connector classes by Overriding
        `self._store_file( .. )` and `self._store_dir( .. )`

        Parameters
        ----------
        :param reference: Path to either a `File` or `Directory`
        :type  reference: str

        :param filename: Set the name of stored file, instead of uuid
        :type  filename: str

        :param subdir: Store a file under this sub directory path
        :type  subdir: str

        :param arcname: If given, `arcname' set an alternative
                        name for the file in the archive.
        :type arcname: str

        :param suffix: Set the filename extension defaults to `tar.gz`
        :type suffix: str

        :return: access storage reference returned from self._store_file, self._store_dir
                 This will either be a pre-signed URL or absolute filepath
        :rtype str
        """
        if not reference:
            return None

        if os.path.isfile(reference):
            ext = "".join(Path(reference).suffixes) if not suffix else suffix
            filename = filename if filename else self._get_unique_filename(ext)
            storage_path = subdir if subdir else ''
            self.fs.mkdirs(os.path.dirname(storage_path), exist_ok=True)
            storage_location = os.path.join(storage_path, filename)

            self.logger.info("Store file: {} -> {}".format(reference, storage_location))
            self.fs.put(reference, storage_location)
            return storage_location

        elif os.path.isdir(reference):
            ext = "tar.gz" if not suffix else suffix
            filename = filename if filename else self._get_unique_filename(ext)
            storage_path = os.path.join(subdir, filename) if subdir else filename
            self.fs.mkdirs(os.path.dirname(storage_path), exist_ok=True)

            self.logger.info("Store dir: {} -> {}".format(reference, storage_path))
            with tempfile.NamedTemporaryFile() as f:
                self.compress(f.name, reference, arcname)
                self.fs.put(f.name, storage_path)
            return storage_path
        else:
            return None

    def delete_file(self, reference):
        """
        Delete single file from shared storage

        :param reference: Path to `File`
        :type  reference: str
        """
        if self.fs.isfile(reference):
            self.fs.delete(reference)
            logging.info("Deleted Shared file: {}".format(reference))
        else:
            logging.info("Delete Error - Unknwon reference {}".format(reference))

    def delete_dir(self, reference):
        """
        Delete subdirectory from shared storage

        :param reference: Path to `Directory`
        :type  reference: str
        """
        if self.fs.isdir(reference):
            if Path("/") == Path(reference).resolve():
                logging.info("Delete Error - prevented media root deletion")
            else:
                self.fs.delete(reference, recursive=True)
                logging.info("Deleted shared dir: {}".format(reference))
        else:
            logging.info("Delete Error - Unknwon reference {}".format(reference))

    def create_traceback(self, stdout, stderr, output_dir=""):
        traceback_file = self._get_unique_filename(LOG_FILE_SUFFIX)
        with tempfile.NamedTemporaryFile("w") as f:
            if stdout:
                f.write(stdout)
            if stderr:
                f.write(stderr)

            self.put(f.name, filename=traceback_file)
        return traceback_file

    def get_storage_url(
        self, filename=None, suffix="tar.gz", encode_params=True
    ) -> Tuple[str, str]:
        raise NotImplementedError

    def get_fsspec_storage_options(self):
        return {}

    @property
    def fs(self) -> fsspec.AbstractFileSystem:
        if not self._fs:
            self._fs = StrictRootDirFs(
                path=self.root_dir,
                fs=(
                    self.fsspec_filesystem_class(**self.get_fsspec_storage_options())
                    if self.fsspec_filesystem_class
                    else None
                ),
            )
        return self._fs

    def exists(self, path):
        return self.fs.exists(path)

    def isfile(self, path):
        return self.fs.isfile(path)

    def listdir(self, path=""):
        return self.fs.listdir(path, detail=False)

    @contextlib.contextmanager
    def open(self, path, *args, **kwargs):
        if self._is_valid_url(path):
            with tempfile.TemporaryDirectory() as d:
                with open(
                    self.get_from_cache(path, no_cache_target=os.path.join(d, "f"))
                ) as f:
                    yield f
        else:
            with self.fs.open(path, *args, **kwargs) as f:
                yield f

    @contextlib.contextmanager
    def with_fileno(self, path, mode="rb"):
        with tempfile.TemporaryDirectory() as d:
            target = os.path.join(d, "fileno")
            path = self.get_from_cache(path, no_cache_target=target)

            with open(path, mode) as f:
                yield f
