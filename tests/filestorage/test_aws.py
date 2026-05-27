import os.path
import tempfile
import uuid

import pytest
import urllib3
from fsspec.asyn import sync

from oasis_data_manager.filestore.backends.aws import AwsS3Storage
from oasis_data_manager.filestore.config import get_storage_from_config


def make_storage(**kwargs):
    kwargs.setdefault("bucket_name", uuid.uuid4().hex)
    kwargs.setdefault("access_key", "LSIAQAAAAAAVNCBMPNSG")
    kwargs.setdefault("secret_key", "ANYTHING")
    kwargs.setdefault("endpoint_url", "http://localhost:4566")
    kwargs.setdefault("cache_dir", None)
    kwargs.setdefault("region_name", "eu-central-1")

    fs = AwsS3Storage(**kwargs)
    fs.fs.mkdirs("")

    return fs


def test_no_root_dir_is_set___root_dir_is_empty():
    storage = make_storage()

    assert storage.root_dir == storage.bucket_name


def test_storage_constructed_from_config_matches_initial():
    storage = make_storage(root_dir="test_root")

    result = get_storage_from_config(storage.to_config())

    assert storage.root_dir == os.path.join(storage.bucket_name, "test_root")
    assert isinstance(result, AwsS3Storage)
    assert result.root_dir == storage.root_dir
    assert result.bucket_name == storage.bucket_name


def test_location_does_not_affect_root_dir():
    # root_dir is bucket-scoped only; location is applied at put/get_storage_url time
    storage = make_storage(location="oasis")

    assert storage.root_dir == storage.bucket_name


def test_location_round_trips_through_config():
    storage = make_storage(location="oasis", root_dir="subdir")

    result = get_storage_from_config(storage.to_config())

    assert isinstance(result, AwsS3Storage)
    assert result.root_dir == storage.root_dir
    assert result.location == storage.location


def test_location_put_returns_bucket_relative_key():
    # The returned key must be the raw S3 key (bucket-relative) so the server's
    # is_in_bucket() lookup and CopyObject both resolve correctly.
    storage = make_storage(location="oasis")

    with tempfile.NamedTemporaryFile("w", suffix=".txt") as f:
        f.write("content")
        f.flush()

        stored_path = storage.put(f.name, "test_file.txt")

    assert stored_path == os.path.join("oasis", "test_file.txt")
    # The file must actually live at that bucket-relative key
    assert storage.fs.isfile(stored_path)


def test_location_put_with_subdir():
    storage = make_storage(location="oasis")

    with tempfile.NamedTemporaryFile("w", suffix=".txt") as f:
        f.write("content")
        f.flush()

        stored_path = storage.put(f.name, "test_file.txt", subdir="run1")

    assert stored_path == os.path.join("oasis", "run1", "test_file.txt")
    assert storage.fs.isfile(stored_path)


def test_no_location_put_returns_bare_key():
    # Without location, behaviour is unchanged: returned key has no prefix
    storage = make_storage()

    with tempfile.NamedTemporaryFile("w", suffix=".txt") as f:
        f.write("content")
        f.flush()

        stored_path = storage.put(f.name, "test_file.txt")

    assert stored_path == "test_file.txt"
    assert storage.fs.isfile(stored_path)


def test_location_get_storage_url_includes_prefix():
    storage = make_storage(location="oasis")

    key, url = storage.get_storage_url(filename="myfile.tar.gz")

    assert key == "oasis/myfile.tar.gz"
    assert "oasis/myfile.tar.gz" in url


def test_location_put_then_get_roundtrip():
    # Worker puts a file, then retrieves it using the returned key — must work
    storage = make_storage(location="oasis")

    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as src:
        src.write("hello")
        src.flush()
        stored_path = storage.put(src.name, "roundtrip.txt")

    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as dest:
        dest_path = dest.name

    result = storage.get(stored_path, dest_path)
    assert result == dest_path
    with open(dest_path) as f:
        assert f.read() == "hello"


@pytest.mark.parametrize("acl", ["public-read-write", "public-read"])
def test_uploaded_file_has_the_correct_acl(acl):
    storage = make_storage(default_acl=acl)

    with tempfile.NamedTemporaryFile("w") as f:
        f.write("content")
        f.flush()

        storage.put(f.name, "test_file")

    res = sync(
        storage.fs.fs.loop,
        storage.fs.fs.s3.get_object_acl,
        Bucket=storage.bucket_name,
        Key="test_file",
    )

    writable = acl == "public-read-write"
    assert {
        "Grantee": {
            "Type": "Group",
            "URI": "http://acs.amazonaws.com/groups/global/AllUsers",
        },
        "Permission": "READ",
    } in res["Grants"]
    assert writable == (
        {
            "Grantee": {
                "Type": "Group",
                "URI": "http://acs.amazonaws.com/groups/global/AllUsers",
            },
            "Permission": "WRITE",
        }
        in res["Grants"]
    )


@pytest.mark.parametrize("acl", ["public-read-write", "public-read"])
def test_written_file_has_the_correct_acl(acl):
    storage = make_storage(default_acl=acl)

    with storage.open("test_file", "w") as f:
        f.write("content")

    res = sync(
        storage.fs.fs.loop,
        storage.fs.fs.s3.get_object_acl,
        Bucket=storage.bucket_name,
        Key="test_file",
    )

    writable = acl == "public-read-write"
    assert {
        "Grantee": {
            "Type": "Group",
            "URI": "http://acs.amazonaws.com/groups/global/AllUsers",
        },
        "Permission": "READ",
    } in res["Grants"]
    assert writable == (
        {
            "Grantee": {
                "Type": "Group",
                "URI": "http://acs.amazonaws.com/groups/global/AllUsers",
            },
            "Permission": "WRITE",
        }
        in res["Grants"]
    )


def test_presigned_url():
    storage = make_storage(querystring_auth=True, default_acl="public-read")

    with storage.open("test_file", "w") as f:
        f.write("content")

    url = storage.url("test_file")

    res = urllib3.PoolManager().request("GET", url)
    assert res.data.decode() == "content"
