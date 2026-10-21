"""Tar archive helpers used by the storage backends to `put` and `extract` directories

Archives are written as gzip (``tar.gz``, the default) or zstandard (``tar.zst``).
zstd uses the standard library ``compression.zstd`` module on Python 3.14+, or the
``zstandard`` package (``pip install oasis-data-manager[zstd]``) on older versions.
"""
import contextlib
import os
import tarfile

from oasis_data_manager.errors import OasisDataManagerException

GZIP = "gz"
ZSTD = "zst"
ARCHIVE_SUFFIXES = {GZIP: "tar.gz", ZSTD: "tar.zst"}

# Speed over size: on Oasis inputs gzip level 1 is ~11x faster than level 9 for a ~10% larger archive
DEFAULT_COMPRESSLEVEL = {GZIP: 1, ZSTD: 3}

ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"


def validate_compression(compression):
    if compression not in ARCHIVE_SUFFIXES:
        raise OasisDataManagerException(
            f"Unsupported archive compression '{compression}', expected one of {list(ARCHIVE_SUFFIXES)}"
        )
    return compression


def _zstd_threads(threads):
    """-1 uses every available core, 0 compresses in the calling thread"""
    if threads is None or threads < 0:
        return os.cpu_count() or 1
    return threads


def _stdlib_zstd():
    try:
        from compression import zstd
    except ImportError:
        return None
    return zstd


def _zstandard():
    try:
        import zstandard
    except ImportError:
        raise OasisDataManagerException(
            "zstd archives need Python 3.14+ or the 'zstandard' package, install with 'pip install oasis-data-manager[zstd]'"
        )
    return zstandard


def is_zstd(archive_fp):
    with open(archive_fp, "rb") as f:
        return f.read(4) == ZSTD_MAGIC


def compress_dir(archive_fp, directory, arcname="/", compression=GZIP, compresslevel=None, threads=-1):
    """Write `directory` to a tar archive at `archive_fp`

    :param compression: `gz` or `zst`
    :param compresslevel: compression level, defaults to `DEFAULT_COMPRESSLEVEL` for the format
    :param threads: zstd worker threads, -1 for every core (ignored for gzip)
    """
    validate_compression(compression)
    level = DEFAULT_COMPRESSLEVEL[compression] if compresslevel is None else int(compresslevel)

    if compression == GZIP:
        with tarfile.open(archive_fp, "w:gz", compresslevel=level) as tar:
            tar.add(directory, arcname=arcname)
        return

    threads = _zstd_threads(threads)
    zstd = _stdlib_zstd()
    if zstd:
        options = {zstd.CompressionParameter.compression_level: level}
        if threads and zstd.CompressionParameter.nb_workers.bounds()[1] > 0:
            options[zstd.CompressionParameter.nb_workers] = threads
        with zstd.ZstdFile(archive_fp, "w", options=options) as zf, tarfile.open(fileobj=zf, mode="w|") as tar:
            tar.add(directory, arcname=arcname)
        return

    cctx = _zstandard().ZstdCompressor(level=level, threads=threads)
    with open(archive_fp, "wb") as fh, cctx.stream_writer(fh, closefd=False) as writer, \
            tarfile.open(fileobj=writer, mode="w|") as tar:
        tar.add(directory, arcname=arcname)


@contextlib.contextmanager
def open_archive(archive_fp):
    """Open a gzip, zstd or uncompressed tar archive for reading

    zstd archives are opened as a stream: iterate or `extractall`, don't seek back to earlier members.
    """
    if not is_zstd(archive_fp):
        with tarfile.open(archive_fp) as tar:
            yield tar
        return

    zstd = _stdlib_zstd()
    if zstd:
        with zstd.ZstdFile(archive_fp, "r") as zf, tarfile.open(fileobj=zf, mode="r|") as tar:
            yield tar
        return

    dctx = _zstandard().ZstdDecompressor()
    with open(archive_fp, "rb") as fh, dctx.stream_reader(fh) as reader, tarfile.open(fileobj=reader, mode="r|") as tar:
        yield tar


def extract_archive(archive_fp, directory):
    with open_archive(archive_fp) as tar:
        os.makedirs(directory, exist_ok=True)
        tar.extractall(directory)
