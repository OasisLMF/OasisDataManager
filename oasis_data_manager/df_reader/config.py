import json
from copy import deepcopy
from pathlib import Path
from typing import Union

from ..config import ConfigError, load_class
from ..filestore.backends.local import LocalStorage
from .reader import OasisReader


def clean_config(config: Union[str, dict]) -> dict:
    if isinstance(config, (str, Path)) or hasattr(config, "read"):
        _config: dict = {
            "filepath": config,
        }
    elif not isinstance(config, dict):
        raise ConfigError(f"df_reader config must be a string or dictionary: {config}")
    else:
        _config = deepcopy(config)

    if "filepath" not in _config:
        raise ConfigError(
            f"df_reader config must provide a 'filepath' property: {_config}"
        )

    if "engine" not in _config:
        _config["engine"] = {
            "path": "oasis_data_manager.df_reader.reader.OasisPandasReader",
            "options": {},
        }
    elif isinstance(_config.get("engine"), str):
        try:
            # try to decode the string a json object so it can be
            # serialized on the command line
            _config["engine"] = json.loads(_config.get("engine"))  # type: ignore
        except json.JSONDecodeError:
            _config["engine"] = {"path": _config.get("engine"), "options": {}}

    _config["engine"].setdefault("path", "oasis_data_manager.df_reader.reader.OasisPandasReader")
    _config["engine"].setdefault("options", {})

    return _config


def get_df_reader(config, *args, **kwargs):
    config = clean_config(config)
    cls = load_class(config["engine"]["path"], OasisReader)
    storage = config["engine"]["options"].pop("storage", None) or LocalStorage("/")

    return cls(
        config["filepath"], storage, *args, **kwargs, **config["engine"]["options"]
    )
