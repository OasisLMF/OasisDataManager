import os.path
import uuid

import urllib3

from oasis_data_manager.filestore.backends.azure import AzureABFSStorage
from oasis_data_manager.filestore.config import get_storage_from_config


def make_storage(**kwargs):
    kwargs.setdefault("azure_container", uuid.uuid4().hex)
    kwargs.setdefault("account_name", "devstoreaccount1")
    kwargs.setdefault(
        "account_key",
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
    )
    kwargs.setdefault("endpoint_url", "http://localhost:10000/devstoreaccount1")
    kwargs.setdefault("cache_dir", None)

    fs = AzureABFSStorage(**kwargs)
    fs.fs.fs.mkdir(fs.azure_container)
    fs.fs.mkdirs("")

    return fs


def test_no_root_dir_is_set___root_dir_is_empty():
    storage = make_storage()

    assert storage.root_dir == storage.azure_container


def test_storage_constructed_from_config_matches_initial():
    storage = make_storage(root_dir="test_root")

    result = get_storage_from_config(storage.to_config())

    assert storage.root_dir == os.path.join(storage.azure_container, "test_root")
    assert isinstance(result, AzureABFSStorage)
    assert result.root_dir == storage.root_dir
    assert result.azure_container == storage.azure_container


def test_presigned_url():
    storage = make_storage()

    with storage.open("test_file", "w") as f:
        f.write("content")

    url = storage.url("test_file")

    res = urllib3.PoolManager().request("GET", url)
    assert res.data.decode() == "content"


AZURITE_ENV_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://localhost:10000/devstoreaccount1;"
)


def test_no_credentials___connection_string_is_none():
    storage = AzureABFSStorage(azure_container="container")

    assert storage.connection_string is None


def test_no_credentials___storage_url_has_no_connection_string():
    storage = AzureABFSStorage(azure_container="container")

    _, url = storage.get_storage_url("file.tar.gz")

    assert url == "abfs://container/file.tar.gz"


def test_no_credentials___env_connection_string_is_used(monkeypatch):
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", AZURITE_ENV_CONNECTION_STRING)
    container = uuid.uuid4().hex
    make_storage(azure_container=container)  # creates the container with explicit credentials

    storage = AzureABFSStorage(azure_container=container, root_dir="root", cache_dir=None)
    storage.fs.mkdirs("", exist_ok=True)
    with storage.open("test_file", "w") as f:
        f.write("content")

    with storage.open("test_file", "r") as f:
        assert f.read() == "content"
