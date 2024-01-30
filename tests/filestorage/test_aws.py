import os.path
import tempfile
import uuid

import pytest
import urllib3
from fsspec.asyn import sync

from oasis_data_manager.filestore.backends.aws_s3 import AwsS3Storage
from oasis_data_manager.filestore.config import get_storage_from_config


def make_storage(**kwargs):
    kwargs.setdefault("bucket_name", uuid.uuid4().hex)
    kwargs.setdefault("access_key", "LSIAQAAAAAAVNCBMPNSG")
    kwargs.setdefault("secret_key", "ANYTHING")
    kwargs.setdefault("endpoint_url", "http://localhost:4566")
    kwargs.setdefault("cache_dir", None)

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
