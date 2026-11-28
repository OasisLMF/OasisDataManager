"""load_class names the package to install when a backend's optional dependency is missing (#4)."""
import sys

import fsspec  # noqa: F401, loads fsspec.registry
import pytest

from oasis_data_manager.config import ConfigError, MissingDependencyError, load_class
from oasis_data_manager.filestore.backends.local import LocalStorage


def block(monkeypatch, *modules, unload=()):
    """Make importing ``modules`` fail, and unload ``unload`` so its imports run again."""
    for module in modules:
        monkeypatch.setitem(sys.modules, module, None)
    for module in unload:
        monkeypatch.delitem(sys.modules, module, raising=False)


@pytest.mark.parametrize("path", [
    "OasisDaskReader",
    "oasis_data_manager.df_reader.backends.dask.OasisDaskReaderCSV",
    # the reader module only exports the dask readers when dask is installed
    "oasis_data_manager.df_reader.reader.OasisDaskReader",
])
def test_missing_dask(monkeypatch, path):
    block(monkeypatch, "dask", unload=["oasis_data_manager.df_reader.backends.dask", "oasis_data_manager.df_reader.reader"])
    with pytest.raises(MissingDependencyError, match=r"pip install 'oasis-data-manager\[dask\]'"):
        load_class(path)


def test_missing_pyarrow(monkeypatch):
    block(monkeypatch, "pyarrow", "pyarrow.dataset", unload=["oasis_data_manager.df_reader.backends.pyarrow"])
    with pytest.raises(MissingDependencyError, match="pip install 'pyarrow'"):
        load_class("OasisPyarrowReader")


@pytest.mark.parametrize("path, module, protocol, package, extra", [
    ("AwsS3Storage", "oasis_data_manager.filestore.backends.aws", "s3", "s3fs", "s3"),
    ("AzureABFSStorage", "oasis_data_manager.filestore.backends.azure", "abfs", "adlfs", "azure"),
])
def test_missing_object_store(monkeypatch, path, module, protocol, package, extra):
    block(monkeypatch, package, unload=[module])
    # fsspec caches the filesystem classes it has imported (fsspec.registry is shadowed by a mapping of them)
    monkeypatch.delitem(sys.modules["fsspec.registry"]._registry, protocol, raising=False)
    with pytest.raises(MissingDependencyError, match=rf"pip install 'oasis-data-manager\[{extra}\]'"):
        load_class(path)


def test_missing_dependency_error_is_a_config_and_import_error():
    assert issubclass(MissingDependencyError, ConfigError)
    assert issubclass(MissingDependencyError, ImportError)


def test_installed_backend_loads():
    assert load_class("LocalStorage") is LocalStorage


def test_unknown_module_is_not_reported_as_a_missing_dependency():
    with pytest.raises(ModuleNotFoundError) as exc_info:
        load_class("no_such_package.Storage")
    assert not isinstance(exc_info.value, MissingDependencyError)


def test_unknown_class_is_not_reported_as_a_missing_dependency():
    with pytest.raises(AttributeError):
        load_class("oasis_data_manager.filestore.backends.local.NoSuchStorage")
