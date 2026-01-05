from dataclasses import dataclass
import logging
from typing import Callable
import uuid
import boto3
import os
import pytest
import tempfile
from pathlib import Path
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError

from oasis_data_manager.errors import OasisException
from oasis_data_manager.filestore.backends.base import BaseStorage
from oasis_data_manager.filestore.backends.aws_s3 import AwsS3Storage
from oasis_data_manager.filestore.backends.azure_abfs import AzureABFSStorage
from oasis_data_manager.filestore.backends.base import MissingInputsException


@dataclass
class StorageContext:
    storage: BaseStorage
    upload_file: Callable[[str, bytes], None]  # upload file function


@pytest.fixture
def storage_context(request):
    config = request.param
    backend_type = config["backend"]
    temp_cache = tempfile.TemporaryDirectory()
    cache_dir = (
        temp_cache.name
        if "cache_dir" not in config or config["cache_dir"] is not None
        else None
    )

    if backend_type == "s3":
        localstack_endpoint = "http://localhost:4566"
        bucket_name = f"test-bucket-{uuid.uuid4().hex[:8]}"

        # boto3 client/resource pointing to LocalStack
        s3 = boto3.resource(
            "s3",
            endpoint_url=localstack_endpoint,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="eu-west-2",
        )
        # Create bucket
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        def s3_upload_file(key, content):
            s3.Object(bucket_name, key).put(Body=content)

        # Storage instance
        storage = AwsS3Storage(
            bucket_name=bucket_name,
            root_dir="",
            cache_dir=cache_dir,
            endpoint_url=localstack_endpoint,
            access_key="test",
            secret_key="test",
            region_name="eu-west-2",
        )

        yield StorageContext(storage=storage, upload_file=s3_upload_file)

        # Clean up
        bucket = s3.Bucket(bucket_name)
        bucket.objects.all().delete()
        bucket.delete()
        temp_cache.cleanup()
    elif backend_type == "azure":
        azurite_acc_name = "devstoreaccount1"
        azurite_acc_key = (
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
        )
        azurite_endpoint = "http://127.0.0.1:10000"
        container_name = f"test-container-{uuid.uuid4().hex[:8]}"

        # Blob service client (Azurite)
        blob_service = BlobServiceClient(
            account_url=f"{azurite_endpoint}/{azurite_acc_name}",
            credential=azurite_acc_key,
        )

        # Create container
        try:
            blob_service.create_container(container_name)
            print(f"Container '{container_name}' created successfully.")
        except ResourceExistsError:
            print(f"Container '{container_name}' already exists.")
        except Exception as e:
            raise OasisException(f"An error occurred for creating the azurite container: {e}")
        container_client = blob_service.get_container_client(container_name)

        def azure_upload_file(key, content):
            container_client.upload_blob(key, content, overwrite=True)

        # Storage instance
        storage = AzureABFSStorage(
            account_name=azurite_acc_name,
            account_key=azurite_acc_key,
            azure_container=container_name,
            endpoint_url=azurite_endpoint,
            cache_dir=cache_dir,
            root_dir="",
        )

        yield StorageContext(storage=storage, upload_file=azure_upload_file)

        for blob in container_client.list_blobs():
            container_client.delete_blob(blob.name)
        container_client.delete_container()
        temp_cache.cleanup()
    else:
        raise OasisException(f"Unsupported backend_type ({backend_type}) for testing")


@pytest.mark.parametrize("storage_context", [
    {"backend": "s3"},
    {"backend": "azure"},
], indirect=True)
def test_first_fetch_downloads_and_caches(storage_context):
    """Test basic download and caching"""
    key = "test/file.txt"
    content = b"hello world"

    storage_context.upload_file(key, content)

    cached_path = storage_context.storage.get_from_cache(key)
    assert cached_path
    assert Path(cached_path).exists()
    assert Path(cached_path).read_bytes() == content


@pytest.mark.parametrize("storage_context", [
    {"backend": "s3"},
    {"backend": "azure"},
], indirect=True)
def test_cache_hit_returns_same_file(storage_context):
    """Test that fetching the same file again hits the cache (ETag match)"""
    key = "test/file2.txt"
    content = b"cached content"

    storage_context.upload_file(key, content)

    # First fetch
    cached_path_1 = storage_context.storage.get_from_cache(key)
    # Second fetch
    cached_path_2 = storage_context.storage.get_from_cache(key)

    assert cached_path_1 == cached_path_2
    assert Path(cached_path_2).read_bytes() == content


@pytest.mark.parametrize("storage_context", [
    {"backend": "s3"},
    {"backend": "azure"},
], indirect=True)
def test_cache_miss_on_etag_change(storage_context):
    """Test that changing file in S3 updates the cache"""
    key = "test/file3.txt"
    content1 = b"first version"
    content2 = b"second version"

    storage_context.upload_file(key, content1)

    cached_path_1 = storage_context.storage.get_from_cache(key)

    # Overwrite the S3 object
    storage_context.upload_file(key, content2)

    cached_path_2 = storage_context.storage.get_from_cache(key)

    assert Path(cached_path_1).parent == Path(cached_path_2).parent
    assert Path(cached_path_2).read_bytes() == content2


@pytest.mark.parametrize("storage_context", [
    {"backend": "s3"},
    {"backend": "azure"},
], indirect=True)
def test_missing_file_raises_exception_when_required(storage_context):
    """Test that requesting a missing file with required=True raises exception"""
    key = "nonexistent/file.txt"
    with pytest.raises(MissingInputsException):
        storage_context.storage.get_from_cache(key, required=True)


@pytest.mark.parametrize("storage_context", [
    {"backend": "s3"},
    {"backend": "azure"},
], indirect=True)
def test_missing_file_returns_none_when_not_required(storage_context):
    """Test that requesting a missing file with required=False returns None"""
    key = "nonexistent/file.txt"
    result = storage_context.storage.get_from_cache(key, required=False)
    assert result == None


@pytest.mark.parametrize("storage_context", [
    {"backend": "s3"},
    {"backend": "azure"},
], indirect=True)
def test_missing_etag_skips_cache(storage_context, caplog):
    """Test missing etag skips hashing and returns file"""
    key = "noetag/file.txt"
    content = b"something"

    storage_context.upload_file(key, content)

    # Patch fs.info to simulate no ETag
    original_info = storage_context.storage.fs.fs.info

    def fake_info(path):
        d = original_info(path)
        d.pop("ETag", None)
        d.pop("etag", None)
        return d

    storage_context.storage.fs.fs.info = fake_info

    caplog.set_level(logging.WARNING)
    result = storage_context.storage.get_from_cache(key)

    # Check warning message output
    assert f"ETag missing for {key} — skipping cache and returning fresh download" in caplog.text

    # Cache dir should NOT contain ref_hash folder
    assert os.listdir(storage_context.storage.cache_root) == []
    assert Path(result).read_bytes() == content


@pytest.mark.parametrize("storage_context", [
    {"backend": "s3", "cache_dir": None},
    {"backend": "azure", "cache_dir": None},
], indirect=True)
def test_no_cache_target_required_when_cache_disabled(storage_context):
    """Test no_cache_target=None throws exception when cache_dir is None"""
    with pytest.raises(OasisException):
        storage_context.storage.get_from_cache("anything", no_cache_target=None)


@pytest.mark.parametrize("storage_context", [
    {"backend": "s3"},
    {"backend": "azure"},
], indirect=True)
def test_get_from_cache_directory_raises_error(storage_context):
    """Test get directory raises error"""
    prefix = "somedir/"
    key = f"{prefix}file.txt"
    content = b"x"
    storage_context.upload_file(key, content)

    with pytest.raises(OasisException):
        storage_context.storage.get_from_cache(prefix)
