import logging
import os
from pathlib import Path
from typing import Optional
from urllib import parse

import fsspec

from ..log import set_azure_log_level
from .base import BaseStorage


class AzureABFSStorage(BaseStorage):
    # https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python
    fsspec_filesystem_class = fsspec.get_filesystem_class("abfs")

    def __init__(
        self,
        account_name: Optional[str] = None,
        account_key: Optional[str] = None,
        azure_container: Optional[str] = None,
        location="",
        connection_string: Optional[str] = None,
        shared_container=True,
        azure_ssl=True,
        upload_max_conn=2,
        timeout=20,
        max_memory_size=2 * 1024 * 1024,
        expiration_secs: Optional[int] = None,
        overwrite_files=True,
        default_content_type="application/octet-stream",
        cache_control: Optional[str] = None,
        sas_token: Optional[str] = None,
        custom_domain: Optional[str] = None,
        token_credential: Optional[str] = None,
        azure_log_level=logging.ERROR,
        root_dir="",
        endpoint_url: Optional[str] = None,
        **kwargs,
    ):
        self._service_client = None
        self._client = None

        # Required
        self.account_name = account_name
        self.account_key = account_key
        self.azure_container = azure_container

        # Optional
        self._connection_string = connection_string
        self.shared_container = shared_container
        self.azure_ssl = azure_ssl
        self.upload_max_conn = upload_max_conn
        self.timeout = timeout
        self.max_memory_size = max_memory_size
        self.expiration_secs = expiration_secs
        self.overwrite_files = overwrite_files
        self.default_content_type = default_content_type
        self.cache_control = cache_control
        self.sas_token = sas_token
        self.custom_domain = custom_domain
        self.token_credential = token_credential
        self.azure_log_level = azure_log_level
        self.azure_protocol = "https" if self.azure_ssl else "http"
        self.endpoint_url = endpoint_url
        set_azure_log_level(self.azure_log_level)

        root_dir = os.path.join(self.azure_container or "", root_dir or location or "")
        if root_dir.startswith(os.path.sep):
            root_dir = root_dir[1:]
        if root_dir.endswith(os.path.sep):
            root_dir = root_dir[:-1]

        super(AzureABFSStorage, self).__init__(root_dir=root_dir, **kwargs)

    @property
    def config_options(self):
        return {
            "account_name": self.account_name,
            "account_key": self.account_key,
            "azure_container": self.azure_container,
            "connection_string": self.connection_string,
            "shared_container": self.shared_container,
            "azure_ssl": self.azure_ssl,
            "upload_max_conn": self.upload_max_conn,
            "timeout": self.timeout,
            "max_memory_size": self.max_memory_size,
            "expiration_secs": self.expiration_secs,
            "overwrite_files": self.overwrite_files,
            "default_content_type": self.default_content_type,
            "cache_control": self.cache_control,
            "sas_token": self.sas_token,
            "custom_domain": self.custom_domain,
            "token_credential": self.token_credential,
            "azure_log_level": self.azure_log_level,
            "root_dir": str(Path(self.root_dir).relative_to(self.azure_container)),
            "endpoint_url": self.endpoint_url,
        }

    def url(self, object_name, parameters=None, expire=None):
        blob_key = self.fs._join(object_name)
        return self.fs.fs.url(blob_key)

    def get_fsspec_storage_options(self):
        return {
            "anon": not self.account_key,
            "connection_string": self.connection_string,
            "account_name": self.account_name,
            "account_key": self.account_key,
            "use_ssl": self.azure_ssl,
        }

    @property
    def connection_string(self):
        if self._connection_string:
            return self._connection_string
        else:
            fsspec_storage_options = {
                "anon": not self.account_key,
                "account_name": self.account_name,
                "account_key": self.account_key,
                "use_ssl": self.azure_ssl,
            }
            fs = self.fsspec_filesystem_class(**fsspec_storage_options)

            cs = ""
            if self.endpoint_url:
                cs += f"BlobEndpoint={self.endpoint_url};"
            if fs.account_name:
                cs += f"AccountName={fs.account_name};"
            if fs.account_key:
                cs += f"AccountKey={fs.account_key};"

            return cs

    def get_storage_url(self, filename=None, suffix="tar.gz", encode_params=True):
        filename = (
            filename if filename is not None else self._get_unique_filename(suffix)
        )

        params = {}
        if encode_params:
            params["connection_string"] = self.connection_string

        return (
            filename,
            f"abfs://{os.path.join(self.root_dir, filename)}{'?' if params else ''}{parse.urlencode(params) if params else ''}",
        )
