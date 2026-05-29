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


class ConfigError(Exception):
    pass


def load_class(path, base=None):
    path = _ALIASES.get(path, path)
    path_split = path.rsplit(".", 1)
    if len(path_split) != 2:
        raise ConfigError(f"'{path}' is not a valid class path (expected 'module.ClassName' or a known alias)")

    module_path, cls_name = path_split
    module = importlib.import_module(module_path)
    cls = getattr(module, cls_name)

    if base and not issubclass(cls, base):
        raise ConfigError(f"'{cls.__name__}' does not extend '{base.__name__}'")

    return cls
