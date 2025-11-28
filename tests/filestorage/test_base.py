import tempfile
from pathlib import Path
import pytest
import xxhash

from oasis_data_manager.errors import OasisException
from oasis_data_manager.filestore.backends.base import BaseStorage


class TestStorage(BaseStorage):
    storage_connector = "local"
    fsspec_filesystem_class = None

    @property
    def config_options(self):
        return {}


def write(path: Path, text: str):
    path.write_text(text)


@pytest.fixture
def temp_dirs():
    with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as cache:
        yield Path(root), Path(cache)


def test_get_from_cache_first_fetch_creates_content_hash_entry(temp_dirs):
    root, cache = temp_dirs
    filename = "file.txt"
    write(root / filename, "hello world")

    storage = TestStorage(root_dir=str(root), cache_dir=str(cache))

    result = storage.get_from_cache(filename)

    # Expected file data
    exp_ref_hash = xxhash.xxh64(filename.encode()).hexdigest()
    exp_ref_file_path = cache / f"{exp_ref_hash}.ref"

    # Check files path exists
    cached_file_path = Path(result)
    assert cached_file_path.exists()
    assert exp_ref_file_path.exists()

    # Cached filename should not be the original name
    assert cached_file_path.name != filename

    # Content file same as returned cached file (the file the .ref points to)
    exp_content_path = Path(exp_ref_file_path.read_text())
    assert exp_content_path == cached_file_path
    assert exp_content_path.read_text() == "hello world"


def test_get_from_cache_returns_existing_content_path_if_same_content(temp_dirs):
    root, cache = temp_dirs
    filename = "file.txt"
    f = root / filename
    write(f, "same content")

    storage = TestStorage(root_dir=str(root), cache_dir=str(cache))

    first = storage.get_from_cache(filename)
    second = storage.get_from_cache(filename)

    assert first == second, "Second fetch should hit cached content hash"


def test_get_from_cache_different_content_produces_new_cache_entry(temp_dirs):
    root, cache = temp_dirs

    f1 = "file1.txt"
    f2 = "file2.txt"
    write(root / f1, "content A")
    write(root / f2, "content B")

    storage = TestStorage(root_dir=str(root), cache_dir=str(cache))

    c1 = storage.get_from_cache(f1)
    c2 = storage.get_from_cache(f2)

    assert c1 != c2, "Different content must produce different cache files"
    assert Path(c1).read_text() == "content A"
    assert Path(c2).read_text() == "content B"


def test_get_from_cache_updates_ref_file_when_content_changes(temp_dirs):
    root, cache = temp_dirs
    filename = "file.txt"
    f = root / filename

    storage = TestStorage(root_dir=str(root), cache_dir=str(cache))

    write(f, "first")
    first_cached = storage.get_from_cache(filename)

    write(f, "second")
    second_cached = storage.get_from_cache(filename)

    assert first_cached != second_cached
    assert Path(second_cached).read_text() == "second"

    # Check .ref file points to new content hash
    ref_hash = xxhash.xxh64(filename.encode()).hexdigest()
    ref_link = cache / f"{ref_hash}.ref"
    assert ref_link.read_text() == second_cached


def test_get_from_cache_no_cache_root_uses_no_cache_target(temp_dirs):
    root, cache = temp_dirs
    filename = "file.txt"
    f = root / filename
    write(f, "hello")

    target = root / "out.txt"
    storage = TestStorage(root_dir=str(root), cache_dir=None)

    result = storage.get_from_cache(filename, no_cache_target=str(target))

    assert result == str(target)
    assert target.exists()
    assert target.read_text() == "hello"


def test_get_from_cache_no_cache_root_missing_target_raises(temp_dirs):
    root, cache = temp_dirs
    f = root / "file.txt"
    write(f, "hello")

    storage = TestStorage(root_dir=str(root), cache_dir=None)

    with pytest.raises(OasisException):
        storage.get_from_cache(str(f))
