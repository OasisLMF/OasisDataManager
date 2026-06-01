"""Tests for BaseStorage.create_traceback and AwsS3Storage._strip_signing_parameters."""
import pytest

from oasis_data_manager.filestore.backends.local import LocalStorage

try:
    from oasis_data_manager.filestore.backends.aws import AwsS3Storage
    _aws_available = True
except (ImportError, ModuleNotFoundError):
    _aws_available = False

requires_aws = pytest.mark.skipif(not _aws_available, reason="s3fs not available")


# ---------------------------------------------------------------------------
# BaseStorage.create_traceback
# ---------------------------------------------------------------------------

def test_create_traceback__stdout_only(tmp_path):
    storage = LocalStorage(root_dir=str(tmp_path))

    filename = storage.create_traceback(stdout="stdout content", stderr=None)

    assert filename.endswith(".txt")
    stored = tmp_path / filename
    assert stored.exists()
    assert "stdout content" in stored.read_text()


def test_create_traceback__stderr_only(tmp_path):
    storage = LocalStorage(root_dir=str(tmp_path))

    filename = storage.create_traceback(stdout=None, stderr="error trace")

    assert filename.endswith(".txt")
    stored = tmp_path / filename
    assert stored.exists()
    assert "error trace" in stored.read_text()


def test_create_traceback__both_streams(tmp_path):
    storage = LocalStorage(root_dir=str(tmp_path))

    filename = storage.create_traceback(stdout="out", stderr="err")

    stored = tmp_path / filename
    content = stored.read_text()
    assert "out" in content
    assert "err" in content


def test_create_traceback__returns_unique_filenames(tmp_path):
    storage = LocalStorage(root_dir=str(tmp_path))

    f1 = storage.create_traceback(stdout="a", stderr=None)
    f2 = storage.create_traceback(stdout="b", stderr=None)

    assert f1 != f2


# ---------------------------------------------------------------------------
# AwsS3Storage._strip_signing_parameters
# ---------------------------------------------------------------------------

def _make_s3_storage():
    return AwsS3Storage(
        bucket_name="test-bucket",
        access_key="key",
        secret_key="secret",
        cache_dir=None,
    )


@requires_aws
def test_strip_signing_parameters__removes_v4_params():
    storage = _make_s3_storage()
    signed_url = (
        "https://s3.amazonaws.com/bucket/key"
        "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
        "&X-Amz-Credential=AKID%2F20230101%2Fus-east-1%2Fs3%2Faws4_request"
        "&X-Amz-Date=20230101T000000Z"
        "&X-Amz-Expires=604800"
        "&X-Amz-SignedHeaders=host"
        "&X-Amz-Signature=abc123"
    )

    stripped = storage._strip_signing_parameters(signed_url)

    assert "X-Amz-Algorithm" not in stripped
    assert "X-Amz-Credential" not in stripped
    assert "X-Amz-Signature" not in stripped
    assert "X-Amz-Date" not in stripped
    assert "X-Amz-Expires" not in stripped
    assert "X-Amz-SignedHeaders" not in stripped
    assert "s3.amazonaws.com/bucket/key" in stripped


@requires_aws
def test_strip_signing_parameters__removes_v2_params():
    storage = _make_s3_storage()
    signed_url = (
        "https://s3.amazonaws.com/bucket/key"
        "?AWSAccessKeyId=AKID"
        "&Expires=9999999999"
        "&Signature=xyz456"
    )

    stripped = storage._strip_signing_parameters(signed_url)

    assert "Signature" not in stripped
    assert "Expires" not in stripped
    # AWSAccessKeyId is not in the blacklist (only 'awsaccesskeyid' lowercase), check case-insensitive
    assert "s3.amazonaws.com/bucket/key" in stripped


@requires_aws
def test_strip_signing_parameters__preserves_non_signing_params():
    storage = _make_s3_storage()
    url = "https://s3.amazonaws.com/bucket/key?response-content-type=application%2Fjson"

    stripped = storage._strip_signing_parameters(url)

    assert "response-content-type=application" in stripped


@requires_aws
def test_strip_signing_parameters__no_params_unchanged():
    storage = _make_s3_storage()
    url = "https://s3.amazonaws.com/bucket/key"

    stripped = storage._strip_signing_parameters(url)

    assert stripped == url
