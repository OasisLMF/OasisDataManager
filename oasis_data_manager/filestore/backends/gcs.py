import os
from pathlib import Path
from typing import Optional
from urllib import parse

import fsspec

from ..log import set_gcs_log_level
from .base import BaseStorage


class GcsStorage(BaseStorage):
    fsspec_filesystem_class = fsspec.get_filesystem_class("gcs")
    storage_connector = "GCS"

    def __init__(
        self,
        bucket_name: Optional[str] = None,
        project: Optional[str] = None,
        token: Optional[str] = None,
        access: Optional[str] = "full_control",
        endpoint_url: Optional[str] = None,
        default_location: Optional[str] = None,
        consistency: Optional[str] = None,
        requester_pays: bool = False,
        session_kwargs: Optional[dict] = None,
        root_dir="",
        gcs_log_level="",
        **kwargs,
    ):
        """Storage Connector for Google Cloud Storage

        Store objects in a GCS bucket. Uses gcsfs/fsspec for filesystem access.

        Parameters
        ----------
        :param bucket_name: GCS bucket name
        :type  bucket_name: str

        :param project: GCP project ID
        :type  project: str

        :param token: Authentication method - None (auto-detect), "google_default",
                      "anon", "browser", "cache", or path to service account JSON
        :type  token: str

        :param access: Access level - "read_only", "read_write", "full_control"
        :type  access: str

        :param endpoint_url: Custom endpoint (e.g. for fake-gcs-server emulator)
        :type  endpoint_url: str

        :param default_location: Default bucket location
        :type  default_location: str

        :param consistency: Write check method - "none", "size", "md5", "crc32c"
        :type  consistency: str

        :param requester_pays: Whether the requester pays for requests
        :type  requester_pays: bool

        :param session_kwargs: Dict for aiohttp session (proxy settings etc.)
        :type  session_kwargs: dict
        """
        self._bucket = None
        self.bucket_name = bucket_name

        self.project = project
        self.token = token
        self.access = access
        self.endpoint_url = endpoint_url
        self.default_location = default_location
        self.consistency = consistency
        self.requester_pays = requester_pays
        self.session_kwargs = session_kwargs or {}
        self.gcs_log_level = gcs_log_level
        set_gcs_log_level(self.gcs_log_level)

        root_dir = os.path.join(self.bucket_name or "", root_dir)
        if root_dir.startswith(os.path.sep):
            root_dir = root_dir[1:]
        if root_dir.endswith(os.path.sep):
            root_dir = root_dir[:-1]

        super(GcsStorage, self).__init__(root_dir=root_dir, **kwargs)

    @property
    def config_options(self):
        return {
            "bucket_name": self.bucket_name,
            "project": self.project,
            "token": self.token,
            "access": self.access,
            "endpoint_url": self.endpoint_url,
            "default_location": self.default_location,
            "consistency": self.consistency,
            "requester_pays": self.requester_pays,
            "session_kwargs": self.session_kwargs,
            "root_dir": str(Path(self.root_dir).relative_to(self.bucket_name)),
            "gcs_log_level": self.gcs_log_level,
        }

    def get_fsspec_storage_options(self):
        options = {
            "project": self.project,
            "token": self.token,
            "access": self.access,
            "requester_pays": self.requester_pays,
        }
        if self.endpoint_url:
            options["endpoint_url"] = self.endpoint_url
        if self.default_location:
            options["default_location"] = self.default_location
        if self.consistency:
            options["consistency"] = self.consistency
        if self.session_kwargs:
            options["session_kwargs"] = self.session_kwargs
        return options

    def get_storage_url(self, filename=None, suffix="tar.gz", encode_params=True):
        filename = (
            filename if filename is not None else self._get_unique_filename(suffix)
        )

        params = {}
        if encode_params:
            if self.project:
                params["project"] = self.project

            if self.token:
                params["token"] = self.token

            if self.access:
                params["access"] = self.access

            if self.endpoint_url:
                params["endpoint"] = self.endpoint_url

        return (
            filename,
            f"gs://{os.path.join(self.root_dir, filename)}{'?' if params else ''}{parse.urlencode(params) if params else ''}",
        )

    def url(self, object_name, parameters=None, expire=None):
        """Return URL for object

        Parameters
        ----------
        :param object_name: 'key' or name of object in bucket
        :type  object_name: str

        :param parameters: Dictionary of parameters
        :type  parameters: dict

        :param expire: Time in seconds for the URL to remain valid
        :type  expire: int

        :return: URL as string
        :rtype str
        """
        blob_key = self.fs._join(object_name)
        return self.fs.fs.url(blob_key)
