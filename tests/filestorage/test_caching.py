import logging
import unittest
import boto3
import os
import pytest
import tempfile
from pathlib import Path
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError

from oasis_data_manager.errors import OasisException
from oasis_data_manager.filestore.backends.aws_s3 import AwsS3Storage
from oasis_data_manager.filestore.backends.azure_abfs import AzureABFSStorage
from oasis_data_manager.filestore.backends.base import MissingInputsException


class AWSStorageCacheTests(unittest.TestCase):
    localstack_endpoint = "http://localhost:4566"
    bucket_name = "test-bucket"

    @pytest.fixture(autouse=True)
    def inject_caplog(self, caplog):
        self.caplog = caplog

    def setUp(self):
        # boto3 client/resource pointing to LocalStack
        self.s3 = boto3.resource(
            "s3",
            endpoint_url=self.localstack_endpoint,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="eu-west-2",
        )
        # Create bucket
        self.s3.create_bucket(
            Bucket=self.bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        # Temp cache dir
        self.temp_cache = tempfile.TemporaryDirectory()

        # Storage instance
        self.storage = AwsS3Storage(
            bucket_name=self.bucket_name,
            root_dir="",
            cache_dir=self.temp_cache.name,
            endpoint_url=self.localstack_endpoint,
            access_key="test",
            secret_key="test",
            region_name="eu-west-2",
        )

    def tearDown(self):
        bucket = self.s3.Bucket(self.bucket_name)
        bucket.objects.all().delete()
        bucket.delete()
        self.temp_cache.cleanup()

    def test_first_fetch_downloads_and_caches(self):
        """Test basic download and caching"""
        key = "test/file.txt"
        content = b"hello world"

        self.s3.Object(self.bucket_name, key).put(Body=content)

        cached_path = self.storage.get_from_cache(key)
        self.assertTrue(cached_path and Path(cached_path).exists())
        with open(cached_path, "rb") as f:
            self.assertEqual(f.read(), content)

    def test_cache_hit_returns_same_file(self):
        """Test that fetching the same file again hits the cache (ETag match)"""
        key = "test/file2.txt"
        content = b"cached content"

        obj = self.s3.Object(self.bucket_name, key)
        obj.put(Body=content)

        # First fetch
        cached_path_1 = self.storage.get_from_cache(key)
        # Second fetch
        cached_path_2 = self.storage.get_from_cache(key)

        self.assertEqual(cached_path_1, cached_path_2)
        with open(cached_path_2, "rb") as f:
            self.assertEqual(f.read(), content)

    def test_cache_miss_on_etag_change(self):
        """Test that changing file in S3 updates the cache"""
        key = "test/file3.txt"
        content1 = b"first version"
        content2 = b"second version"

        obj = self.s3.Object(self.bucket_name, key)
        obj.put(Body=content1)

        cached_path_1 = self.storage.get_from_cache(key)

        # Overwrite the S3 object
        obj.put(Body=content2)

        cached_path_2 = self.storage.get_from_cache(key)

        self.assertEqual(Path(cached_path_1).parent, Path(cached_path_2).parent)
        with open(cached_path_2, "rb") as f:
            self.assertEqual(f.read(), content2)

    def test_missing_file_raises_exception_when_required(self):
        """Test that requesting a missing file with required=True raises exception"""
        key = "nonexistent/file.txt"
        with self.assertRaises(MissingInputsException):
            self.storage.get_from_cache(key, required=True)

    def test_missing_file_returns_none_when_not_required(self):
        """Test that requesting a missing file with required=False returns None"""
        key = "nonexistent/file.txt"
        result = self.storage.get_from_cache(key, required=False)
        self.assertIsNone(result)

    def test_missing_etag_skips_cache(self):
        """Test missing etag skips hashing and returns file"""
        key = "noetag/file.txt"
        content = b"something"

        self.s3.Object(self.bucket_name, key).put(Body=content)

        # Patch fs.info to simulate no ETag
        original_info = self.storage.fs.fs.info

        def fake_info(path):
            d = original_info(path)
            d.pop("ETag", None)
            d.pop("etag", None)
            return d

        self.storage.fs.fs.info = fake_info

        self.caplog.set_level(logging.WARNING)
        result = self.storage.get_from_cache(key)

        # Check warning message output
        assert f"ETag missing for {key} — skipping cache and returning fresh download" in self.caplog.text

        # Cache dir should NOT contain ref_hash folder
        self.assertEqual(os.listdir(self.temp_cache.name), [])
        with open(result, "rb") as f:
            self.assertEqual(f.read(), content)

    def test_no_cache_target_required_when_cache_disabled(self):
        """Test no_cache_target=None throws exception when cache_dir is None"""
        no_cache_storage = AwsS3Storage(
            bucket_name=self.bucket_name,
            root_dir="",
            cache_dir=None,
            endpoint_url=self.localstack_endpoint,
            access_key="test",
            secret_key="test",
            region_name="eu-west-2",
        )
        with self.assertRaises(OasisException):
            no_cache_storage.get_from_cache("anything", no_cache_target=None)

    def test_get_from_cache_directory_raises_error(self):
        """Test get directory raises error"""
        prefix = "somedir/"
        self.s3.Object(self.bucket_name, f"{prefix}file.txt").put(Body=b"x")

        with self.assertRaises(OasisException):
            self.storage.get_from_cache(prefix)


class AzureStorageCacheTests(unittest.TestCase):
    azurite_acc_name = "devstoreaccount1"
    azurite_acc_key = (
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
    )
    azurite_endpoint = "http://127.0.0.1:10000"
    container_name = "test-container"

    @pytest.fixture(autouse=True)
    def inject_caplog(self, caplog):
        self.caplog = caplog

    def setUp(self):
        # Blob service client (Azurite)
        self.blob_service = BlobServiceClient(
            account_url=f"{self.azurite_endpoint}/{self.azurite_acc_name}",
            credential=self.azurite_acc_key,
        )

        # Create container
        try:
            self.blob_service.create_container(self.container_name)
            print(f"Container '{self.container_name}' created successfully.")
        except ResourceExistsError:
            print(f"Container '{self.container_name}' already exists.")
        except Exception as e:
            raise OasisException(f"An error occurred for creating the azurite container: {e}")
        self.container_client = self.blob_service.get_container_client(self.container_name)

        # Temp cache dir
        self.temp_cache = tempfile.TemporaryDirectory()

        # Storage instance
        self.storage = AzureABFSStorage(
            account_name=self.azurite_acc_name,
            account_key=self.azurite_acc_key,
            azure_container=self.container_name,
            endpoint_url=self.azurite_endpoint,
            cache_dir=self.temp_cache.name,
            root_dir="",
        )

    def tearDown(self):
        for blob in self.container_client.list_blobs():
            self.container_client.delete_blob(blob.name)
        self.container_client.delete_container()
        self.temp_cache.cleanup()

    def _upload_file(self, name: str, content: bytes):
        self.container_client.upload_blob(name, content, overwrite=True)

    def test_first_fetch_downloads_and_caches(self):
        key = "test/file.txt"
        content = b"hello world"

        self._upload_file(key, content)

        cached_path = self.storage.get_from_cache(key)
        self.assertTrue(cached_path and Path(cached_path).exists())
        self.assertEqual(Path(cached_path).read_bytes(), content)

    def test_cache_hit_returns_same_file(self):
        key = "test/file2.txt"
        content = b"cached content"

        self._upload_file(key, content)

        cached_path_1 = self.storage.get_from_cache(key)
        cached_path_2 = self.storage.get_from_cache(key)

        self.assertEqual(cached_path_1, cached_path_2)
        self.assertEqual(Path(cached_path_2).read_bytes(), content)

    def test_cache_miss_on_etag_change(self):
        key = "test/file3.txt"

        self._upload_file(key, b"first version")
        cached_path_1 = self.storage.get_from_cache(key)

        self._upload_file(key, b"second version")
        cached_path_2 = self.storage.get_from_cache(key)

        self.assertEqual(Path(cached_path_1).parent, Path(cached_path_2).parent)
        self.assertEqual(Path(cached_path_2).read_bytes(), b"second version")

    def test_missing_file_raises_exception_when_required(self):
        with self.assertRaises(MissingInputsException):
            self.storage.get_from_cache("missing/file.txt", required=True)

    def test_missing_file_returns_none_when_not_required(self):
        result = self.storage.get_from_cache("missing/file.txt", required=False)
        self.assertIsNone(result)

    def test_missing_etag_skips_cache(self):
        key = "noetag/file.txt"
        content = b"something"

        self._upload_file(key, content)

        original_info = self.storage.fs.fs.info

        def fake_info(path):
            d = original_info(path)
            d.pop("etag", None)
            d.pop("ETag", None)
            return d

        self.storage.fs.fs.info = fake_info

        self.caplog.set_level(logging.WARNING)
        result = self.storage.get_from_cache(key)

        assert (
            f"ETag missing for {key} — skipping cache and returning fresh download"
            in self.caplog.text
        )
        self.assertEqual(os.listdir(self.temp_cache.name), [])
        self.assertEqual(Path(result).read_bytes(), content)

    def test_no_cache_target_required_when_cache_disabled(self):
        no_cache_storage = AzureABFSStorage(
            account_name=self.azurite_acc_name,
            account_key=self.azurite_acc_key,
            azure_container=self.container_name,
            endpoint_url=self.azurite_endpoint,
            cache_dir=None,
        )
        with self.assertRaises(OasisException):
            no_cache_storage.get_from_cache("anything", no_cache_target=None)

    def test_get_from_cache_directory_raises_error(self):
        self._upload_file("somedir/file.txt", b"x")
        with self.assertRaises(OasisException):
            self.storage.get_from_cache("somedir/")
