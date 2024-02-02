import json
import os
import sys

if sys.version_info >= (3, 8):
    from typing import Optional, Tuple, TypedDict, Union
    from typing_extensions import NotRequired
else:
    from typing import Optional, Tuple, Union
    from typing_extensions import NotRequired, TypedDict

from oasis_data_manager.config import ConfigError, load_class
from oasis_data_manager.filestore.backends.base import BaseStorage
from oasis_data_manager.filestore.backends.local import LocalStorage


class BaseStorageConfig(TypedDict):
    root_dir: str
    cache_dir: str


class LocalStorageConfig(BaseStorageConfig):
    pass


class S3StorageConfig(BaseStorageConfig):
    bucket_name: NotRequired[str]
    access_key: NotRequired[str]
    secret_key: NotRequired[str]
    endpoint_url: NotRequired[str]
    file_overwrite: NotRequired[bool]
    object_parameters: NotRequired[dict]
    auto_create_bucket: NotRequired[bool]
    default_acl: NotRequired[str]
    bucket_acl: NotRequired[str]
    querystring_auth: NotRequired[bool]
    querystring_expire: NotRequired[int]
    reduced_redundancy: NotRequired[bool]
    location: NotRequired[str]
    encryption: NotRequired[bool]
    security_token: NotRequired[str]
    secure_urls: NotRequired[bool]
    file_name_charset: NotRequired[str]
    gzip: NotRequired[bool]
    preload_metadata: NotRequired[bool]
    url_protocol: NotRequired[str]
    region_name: NotRequired[str]
    use_ssl: NotRequired[str]
    verify: NotRequired[bool]
    max_memory_size: NotRequired[int]
    shared_bucket: NotRequired[bool]
    aws_log_level: NotRequired[str]
    gzip_content_types: NotRequired[Tuple[str, ...]]


class AbfsStorageConfig(BaseStorageConfig):
    account_name: NotRequired[str]
    account_key: NotRequired[str]
    azure_container: NotRequired[str]
    location: NotRequired[str]
    connection_string: NotRequired[str]
    shared_container: NotRequired[bool]
    azure_ssl: NotRequired[bool]
    upload_max_conn: NotRequired[int]
    timeout: NotRequired[int]
    max_memory_size: NotRequired[int]
    expiration_secs: NotRequired[int]
    overwrite_files: NotRequired[bool]
    default_content_type: NotRequired[str]
    cache_control: NotRequired[str]
    sas_token: NotRequired[str]
    custom_domain: NotRequired[str]
    token_credential: NotRequired[str]
    azure_log_level: NotRequired[int]
    endpoint_url: NotRequired[str]


class StorageConfig(TypedDict):
    storage_class: str
    options: Union[
        LocalStorageConfig,
        S3StorageConfig,
        AbfsStorageConfig,
    ]


def get_storage_from_config(config: StorageConfig):
    cls = load_class(config["storage_class"], BaseStorage)
    return cls(**config["options"])


def get_storage_from_config_path(config_path: Optional[str], fallback_path: str):
    """
    Loads the config from the supplied path. If no config path is provided or the path
    doesn't exist a local file store object will be created with the root set to the
    fallback path.

    :param config_path: The path to the config file to load
    :param fallback_path: The path for the local file store should the config path not exist
    """
    if config_path and os.path.exists(config_path):
        with open(config_path) as f:
            config: StorageConfig = json.load(f)
            model_storage = get_storage_from_config(config)
    elif fallback_path:
        model_storage = LocalStorage(
            root_dir=fallback_path,
        )
    else:
        raise ConfigError(
            "The given config path does not exist and no fallback path was given to create the local storage from"
        )

    return model_storage
