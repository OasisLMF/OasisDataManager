import os
import tarfile
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from oasis_data_manager.errors import OasisDataManagerException
from oasis_data_manager.filestore import archive
from oasis_data_manager.filestore.archive import compress_dir, extract_archive, is_zstd
from oasis_data_manager.filestore.backends.local import LocalStorage
from oasis_data_manager.filestore.config import get_storage_from_config

try:
    import zstandard  # noqa: F401
    HAS_ZSTANDARD = True
except ImportError:
    HAS_ZSTANDARD = False

HAS_STDLIB_ZSTD = archive._stdlib_zstd() is not None
needs_zstd = pytest.mark.skipif(not (HAS_STDLIB_ZSTD or HAS_ZSTANDARD), reason="zstd needs Python 3.14+ or zstandard")


def make_tree(root):
    Path(root, "sub").mkdir(parents=True)
    Path(root, "a.csv").write_text("loc_id,tiv\n" + "1,100.0\n" * 1000)
    Path(root, "sub", "b.bin").write_bytes(bytes(range(256)) * 64)
    return {"a.csv": Path(root, "a.csv").read_bytes(), "sub/b.bin": Path(root, "sub", "b.bin").read_bytes()}


def assert_tree(root, expected):
    for rel, content in expected.items():
        assert Path(root, rel).read_bytes() == content


@pytest.mark.parametrize("compression", ["gz", pytest.param("zst", marks=needs_zstd)])
def test_compress_then_extract___contents_round_trip(compression):
    with TemporaryDirectory() as src, TemporaryDirectory() as work, TemporaryDirectory() as dst:
        expected = make_tree(src)
        archive_fp = os.path.join(work, "archive")

        compress_dir(archive_fp, src, compression=compression)
        extract_archive(archive_fp, dst)

        assert is_zstd(archive_fp) == (compression == "zst")
        assert_tree(dst, expected)


@pytest.mark.skipif(not HAS_ZSTANDARD, reason="zstandard not installed")
def test_zstd_without_stdlib_module___zstandard_package_is_used(monkeypatch):
    monkeypatch.setattr(archive, "_stdlib_zstd", lambda: None)
    with TemporaryDirectory() as src, TemporaryDirectory() as work, TemporaryDirectory() as dst:
        expected = make_tree(src)
        archive_fp = os.path.join(work, "archive.tar.zst")

        compress_dir(archive_fp, src, compression="zst", threads=2)
        extract_archive(archive_fp, dst)

        assert_tree(dst, expected)


def test_gzip_default_level___fast_level_is_used(monkeypatch):
    levels = []
    real_open = tarfile.open

    def spy_open(*args, **kwargs):
        levels.append(kwargs.get("compresslevel"))
        return real_open(*args, **kwargs)

    monkeypatch.setattr(archive.tarfile, "open", spy_open)
    with TemporaryDirectory() as src, TemporaryDirectory() as work:
        make_tree(src)
        compress_dir(os.path.join(work, "a.tar.gz"), src)
        compress_dir(os.path.join(work, "b.tar.gz"), src, compresslevel=9)

    assert levels == [1, 9]


def test_unknown_compression___raises():
    with pytest.raises(OasisDataManagerException):
        LocalStorage(archive_compression="bz2")


@pytest.mark.parametrize("compression, suffix", [("gz", ".tar.gz"), pytest.param("zst", ".tar.zst", marks=needs_zstd)])
def test_put_directory___archive_uses_storage_compression(compression, suffix):
    with TemporaryDirectory() as root, TemporaryDirectory() as src, TemporaryDirectory() as dst:
        expected = make_tree(src)
        storage = LocalStorage(root_dir=root, cache_dir=None, archive_compression=compression)

        ref = storage.put(src)
        storage.extract(ref, dst)

        assert ref.endswith(suffix)
        assert_tree(dst, expected)


@needs_zstd
def test_put_directory_with_compression_override___override_is_used():
    with TemporaryDirectory() as root, TemporaryDirectory() as src, TemporaryDirectory() as dst:
        expected = make_tree(src)
        storage = LocalStorage(root_dir=root, cache_dir=None)

        ref = storage.put(src, compression="zst")
        storage.extract(ref, dst)

        assert ref.endswith(".tar.zst")
        assert is_zstd(os.path.join(root, ref))
        assert_tree(dst, expected)


def test_storage_from_config___archive_options_are_kept():
    with TemporaryDirectory() as root:
        storage = LocalStorage(root_dir=root, archive_compression="zst", archive_compresslevel=5, archive_threads=4)

        result = get_storage_from_config(storage.to_config())

        assert (result.archive_compression, result.archive_compresslevel, result.archive_threads) == ("zst", 5, 4)
        assert result.archive_suffix() == "tar.zst"


def _archive_with(members_dir, archive_fp, extra=None):
    with tarfile.open(archive_fp, "w:gz") as tar:
        tar.add(members_dir, arcname=".")
        if extra:
            info, data = extra
            tar.addfile(info, data)


@pytest.fixture
def data_filter_default(monkeypatch):
    """Python 3.14's default extraction filter, on any Python that has filters."""
    if not hasattr(tarfile, "data_filter"):
        pytest.skip("tarfile extraction filters not available")
    monkeypatch.setattr(tarfile.TarFile, "extraction_filter", staticmethod(tarfile.data_filter))


def test_extract___absolute_symlinks_are_kept_under_data_filter_default(data_filter_default):
    """OasisPlatform run directories link model data into static/ with absolute symlinks (OasisPlatform#1500)."""
    with TemporaryDirectory() as model_data, TemporaryDirectory() as src, TemporaryDirectory() as work, \
            TemporaryDirectory() as dst:
        target = Path(model_data, "footprint.bin")
        target.write_bytes(b"footprint")
        Path(src, "static").mkdir()
        os.symlink(target, Path(src, "static", "footprint.bin"))
        archive_fp = os.path.join(work, "run.tar.gz")
        _archive_with(src, archive_fp)

        extract_archive(archive_fp, dst)

        link = Path(dst, "static", "footprint.bin")
        assert link.is_symlink() and os.readlink(link) == str(target)
        assert link.read_bytes() == b"footprint"


def test_extract___members_outside_the_directory_are_refused():
    if not hasattr(tarfile, "tar_filter"):
        pytest.skip("tarfile extraction filters not available")
    import io
    with TemporaryDirectory() as root:
        src, dst = Path(root, "src"), Path(root, "dst")
        src.mkdir()
        Path(src, "ok.txt").write_text("ok")
        info = tarfile.TarInfo("../escape.txt")
        info.size = 3
        archive_fp = os.path.join(root, "bad.tar.gz")
        _archive_with(src, archive_fp, extra=(info, io.BytesIO(b"bad")))

        with pytest.raises(tarfile.OutsideDestinationError):
            extract_archive(archive_fp, dst)
        assert not Path(root, "escape.txt").exists()
