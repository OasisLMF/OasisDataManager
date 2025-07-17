import contextlib
import os
from pathlib import Path
from typing import Optional
from urllib import parse
from urllib.parse import parse_qsl, urlsplit

import fsspec
from fsspec.asyn import sync

from ..log import set_aws_log_level
from .base import BaseStorage


class AwsS3Storage(BaseStorage):
    fsspec_filesystem_class = fsspec.get_filesystem_class("s3")

    def __init__(
        self,
        bucket_name: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        file_overwrite=True,
        object_parameters: Optional[dict] = None,
        auto_create_bucket=False,
        default_acl: Optional[str] = None,
        bucket_acl: Optional[str] = None,
        querystring_auth=False,
        querystring_expire=604800,
        reduced_redundancy=False,
        location="",
        encryption=False,
        security_token=None,
        secure_urls=True,
        file_name_charset="utf-8",
        gzip=False,
        preload_metadata=False,
        url_protocol="http:",
        region_name=None,
        use_ssl=True,
        verify=None,
        max_memory_size=0,
        shared_bucket=False,
        aws_log_level="",
        gzip_content_types=(
            "text/css",
            "text/javascript",
            "application/javascript",
            "application/x-javascript",
            "image/svg+xml",
        ),
        root_dir="",
        **kwargs,
    ):
        """Storage Connector for Amazon S3

        Store objects in a bucket common to a single worker pool. Returns a pre-signed URL
        as a response to the server which is downloaded and stored by Django-storage module

        Documentation
        -------------
        https://github.com/jschneier/django-storages/blob/master/storages/backends/s3boto3.py
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#id244

        TODO
        ----

        * Add optional local caching
        * option to set object expiry policy on bucket

            def _get_bucket_policy(self):
                pass
            def _set_lifecycle(self, ):
                pass
                https://stackoverflow.com/questions/14969273/s3-object-expiration-using-boto
                https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-bucket-policies.html

        Parameters
        ----------
        :param settings_conf: Settings object for worker
        :type settings_conf: src.conf.iniconf.Settings
        """
        bucket_acl = bucket_acl or default_acl
        object_parameters = object_parameters or {}

        # Required
        self.storage_connector = "AWS-S3"
        self._bucket = None
        self._connection = None
        self.bucket_name = bucket_name

        # Optional
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url
        self.file_overwrite = file_overwrite
        self.object_parameters = object_parameters
        self.auto_create_bucket = auto_create_bucket
        self.default_acl = default_acl
        self.bucket_acl = bucket_acl
        self.querystring_auth = querystring_auth
        self.querystring_expire = querystring_expire
        self.reduced_redundancy = reduced_redundancy
        self.location = location
        self.encryption = encryption
        self.security_token = security_token
        self.secure_urls = secure_urls
        self.file_name_charset = file_name_charset
        self.gzip = gzip
        self.preload_metadata = preload_metadata
        self.url_protocol = url_protocol
        self.region_name = region_name
        self.use_ssl = use_ssl
        self.verify = verify
        self.max_memory_size = max_memory_size
        self.shared_bucket = shared_bucket
        self.aws_log_level = aws_log_level
        self.gzip_content_types = gzip_content_types
        set_aws_log_level(self.aws_log_level)

        root_dir = os.path.join(self.bucket_name or "", root_dir)
        if root_dir.startswith(os.path.sep):
            root_dir = root_dir[1:]
        if root_dir.endswith(os.path.sep):
            root_dir = root_dir[:-1]

        super(AwsS3Storage, self).__init__(root_dir=root_dir, **kwargs)

    @property
    def config_options(self):
        return {
            "bucket_name": self.bucket_name,
            "access_key": self.access_key,
            "secret_key": self.secret_key,
            "endpoint_url": self.endpoint_url,
            "file_overwrite": self.file_overwrite,
            "object_parameters": self.object_parameters,
            "auto_create_bucket": self.auto_create_bucket,
            "default_acl": self.default_acl,
            "bucket_acl": self.bucket_acl,
            "querystring_auth": self.querystring_auth,
            "querystring_expire": self.querystring_expire,
            "reduced_redundancy": self.reduced_redundancy,
            "location": self.location,
            "encryption": self.encryption,
            "security_token": self.security_token,
            "secure_urls": self.secure_urls,
            "file_name_charset": self.file_name_charset,
            "gzip": self.gzip,
            "preload_metadata": self.preload_metadata,
            "url_protocol": self.url_protocol,
            "region_name": self.region_name,
            "use_ssl": self.use_ssl,
            "verify": self.verify,
            "max_memory_size": self.max_memory_size,
            "shared_bucket": self.shared_bucket,
            "aws_log_level": self.aws_log_level,
            "root_dir": str(Path(self.root_dir).relative_to(self.bucket_name)),
            "gzip_content_types": self.gzip_content_types,
        }

    def get_fsspec_storage_options(self):
        s3_additional_kwargs = {}
        if self.default_acl:
            s3_additional_kwargs["ACL"] = self.default_acl
        if self.encryption:
            s3_additional_kwargs["ServerSideEncryption"] = "AES256"
        if self.reduced_redundancy:
            s3_additional_kwargs["StorageClass"] = "REDUCED_REDUNDANCY"

        return {
            "key": self.access_key,
            "secret": self.secret_key,
            "token": self.security_token,
            "use_ssl": self.use_ssl,
            "s3_additional_kwargs": s3_additional_kwargs,
            "client_kwargs": {
                "endpoint_url": self.endpoint_url,
                "region_name": self.region_name,
            },
        }

    def _strip_signing_parameters(self, url):
        """Duplicated Unsiged URLs from Django-Stroage

        Method from: https://github.com/jschneier/django-storages/blob/master/storages/backends/s3boto3.py

        Boto3 does not currently support generating URLs that are unsigned. Instead we
        take the signed URLs and strip any querystring params related to signing and expiration.
        Note that this may end up with URLs that are still invalid, especially if params are
        passed in that only work with signed URLs, e.g. response header params.
        The code attempts to strip all query parameters that match names of known parameters
        from v2 and v4 signatures, regardless of the actual signature version used.
        """
        split_url = urlsplit(url)
        qs = parse_qsl(split_url.query, keep_blank_values=True)
        blacklist = {
            "x-amz-algorithm",
            "x-amz-credential",
            "x-amz-date",
            "x-amz-expires",
            "x-amz-signedheaders",
            "x-amz-signature",
            "x-amz-security-token",
            "awsaccesskeyid",
            "expires",
            "signature",
        }
        filtered_qs = ((key, val) for key, val in qs if key.lower() not in blacklist)
        # Note: Parameters that did not have a value in the original query string will have
        # an '=' sign appended to it, e.g ?foo&bar becomes ?foo=&bar=
        joined_qs = ("=".join(keyval) for keyval in filtered_qs)
        split_url = split_url._replace(query="&".join(joined_qs))
        return split_url.geturl()

    def url(self, object_name, parameters=None, expire=None):
        """Return Pre-signed URL

        Download URL to `object_name` in the connected bucket with a
        fixed expire time

        Documentation
        -------------
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html


        Parameters
        ----------
        :param object_name: 'key' or name of object in bucket
        :type  object_name: str

        :param parameters: Dictionary of parameters to send to the method (BOTO3)
        :type  parameters: dict

        :param expire: Time in seconds for the presigned URL to remain valid
        :type  expire: int

        :return: Presigned URL as string. If error, returns None.
        :rtype str
        """
        params = parameters.copy() if parameters else {}
        params["Bucket"] = self.bucket_name
        params["Key"] = self.fs._join(object_name).split("/", 1)[
            -1
        ]  # strip the bucket name

        if expire is None:
            expire = self.querystring_expire

        url = sync(
            self.fs.fs.loop,
            self.fs.fs.s3.generate_presigned_url,
            "get_object",
            Params=params,
            ExpiresIn=expire,
        )

        if self.querystring_auth:
            return url
        else:
            return self._strip_signing_parameters(url)

    def get_storage_url(self, filename=None, suffix="tar.gz", encode_params=True):
        filename = (
            filename if filename is not None else self._get_unique_filename(suffix)
        )

        params = {}
        if encode_params:
            if self.default_acl:
                params["acl"] = self.default_acl

            if self.access_key:
                params["key"] = self.access_key

            if self.secret_key:
                params["secret"] = self.secret_key

            if self.security_token:
                params["token"] = self.security_token

            if self.endpoint_url:
                params["endpoint"] = self.endpoint_url

        return (
            filename,
            f"s3://{os.path.join(self.root_dir, filename)}{'?' if params else ''}{parse.urlencode(params) if params else ''}",
        )

    @contextlib.contextmanager
    def open(self, path, *args, **kwargs):
        if self.default_acl:
            kwargs.setdefault("acl", self.default_acl)

        with super().open(path, *args, **kwargs) as f:
            yield f
