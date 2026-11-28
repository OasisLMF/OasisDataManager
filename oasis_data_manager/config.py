import importlib

_ALIASES = {
    # Storage backends
    "LocalStorage": "oasis_data_manager.filestore.backends.local.LocalStorage",
    "AwsS3Storage": "oasis_data_manager.filestore.backends.aws.AwsS3Storage",
    "AzureABFSStorage": "oasis_data_manager.filestore.backends.azure.AzureABFSStorage",
    # Reader backends
    "OasisPandasReader": "oasis_data_manager.df_reader.backends.pandas.OasisPandasReader",
    "OasisPandasReaderCSV": "oasis_data_manager.df_reader.backends.pandas.OasisPandasReaderCSV",
    "OasisPandasReaderParquet": "oasis_data_manager.df_reader.backends.pandas.OasisPandasReaderParquet",
    "OasisDaskReader": "oasis_data_manager.df_reader.backends.dask.OasisDaskReader",
    "OasisDaskReaderCSV": "oasis_data_manager.df_reader.backends.dask.OasisDaskReaderCSV",
    "OasisDaskReaderParquet": "oasis_data_manager.df_reader.backends.dask.OasisDaskReaderParquet",
    "OasisPyarrowReader": "oasis_data_manager.df_reader.backends.pyarrow.OasisPyarrowReader",
}


# Backends that need packages outside the base install, and what to install for them
_OPTIONAL_DEPENDENCIES = {
    "AwsS3Storage": "oasis-data-manager[s3]",
    "AzureABFSStorage": "oasis-data-manager[azure]",
    "OasisDaskReader": "oasis-data-manager[dask]",
    "OasisDaskReaderCSV": "oasis-data-manager[dask]",
    "OasisDaskReaderParquet": "oasis-data-manager[dask]",
    "OasisPyarrowReader": "pyarrow",
}


class ConfigError(Exception):
    pass


class MissingDependencyError(ConfigError, ImportError):
    """A configured backend class needs an optional dependency that isn't installed."""


def load_class(path, base=None):
    path = _ALIASES.get(path, path)
    path_split = path.rsplit(".", 1)
    if len(path_split) != 2:
        raise ConfigError(f"'{path}' is not a valid class path (expected 'module.ClassName' or a known alias)")

    module_path, cls_name = path_split
    try:
        module = importlib.import_module(module_path)
        cls = getattr(module, cls_name)
    except (ImportError, AttributeError) as e:
        # The backend modules import their optional dependencies at the top, and the reader module only
        # exports the backends whose dependencies are installed, so a missing one fails here (#4)
        install = _OPTIONAL_DEPENDENCIES.get(cls_name)
        if install is None:
            raise
        raise MissingDependencyError(
            f"'{path}' needs optional dependencies that are not installed, install them with: pip install '{install}' ({e})"
        ) from e

    if base and not issubclass(cls, base):
        raise ConfigError(f"'{cls.__name__}' does not extend '{base.__name__}'")

    return cls
