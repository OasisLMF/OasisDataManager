import os.path
import uuid

from oasis_data_manager.filestore.backends.gcs import GcsStorage
from oasis_data_manager.filestore.config import get_storage_from_config


def make_storage(**kwargs):
    kwargs.setdefault("bucket_name", uuid.uuid4().hex)
    kwargs.setdefault("token", "anon")
    kwargs.setdefault("endpoint_url", "http://localhost:4443")
    kwargs.setdefault("project", "test-project")
    kwargs.setdefault("cache_dir", None)

    fs = GcsStorage(**kwargs)
    fs.fs.fs.mkdir(fs.bucket_name)
    fs.fs.mkdirs("")

    return fs


def test_no_root_dir_is_set___root_dir_is_empty():
    storage = make_storage()

    assert storage.root_dir == storage.bucket_name


def test_storage_constructed_from_config_matches_initial():
    storage = make_storage(root_dir="test_root")

    result = get_storage_from_config(storage.to_config())

    assert storage.root_dir == os.path.join(storage.bucket_name, "test_root")
    assert isinstance(result, GcsStorage)
    assert result.root_dir == storage.root_dir
    assert result.bucket_name == storage.bucket_name
