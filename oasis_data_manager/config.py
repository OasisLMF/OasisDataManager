import importlib


class ConfigError(Exception):
    pass


def load_class(path, base=None):
    path_split = path.rsplit(".", 1)
    if len(path_split) != 2:
        raise ConfigError(f"'path' found in the df_reader config is not valid: {path}")

    module_path, cls_name = path_split
    module = importlib.import_module(module_path)
    cls = getattr(module, cls_name)

    if base and cls is not base and base not in cls.__bases__:
        raise ConfigError(f"'{cls.__name__}' does not extend '{base.__name__}'")

    return cls
