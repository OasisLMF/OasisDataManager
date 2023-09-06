from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, TypedDict, Union

from typing_extensions import NotRequired

from ..config import ConfigError, load_class
from ..filestore.backends.local_manager import LocalStorageConnector
from .reader import OasisReader


class ResolvedReaderEngineConfig(TypedDict):
    path: str
    options: Dict[str, Any]


class ResolvedReaderConfig(TypedDict):
    filepath: str
    engine: ResolvedReaderEngineConfig


class InputReaderEngineConfig(TypedDict):
    path: NotRequired[str]
    options: NotRequired[Dict[str, Any]]


class InputReaderConfig(TypedDict):
    filepath: str
    engine: NotRequired[Union[str, InputReaderEngineConfig]]


def clean_config(config: Union[str, InputReaderConfig]) -> ResolvedReaderConfig:
    if isinstance(config, (str, Path)) or hasattr(config, "read"):
        _config: dict = {
            "filepath": config,
        }
    elif not isinstance(config, dict):
        raise ConfigError(f"df_reader config must be a string or dictionary: {config}")
    else:
        config: dict  # type: ignore
        _config = deepcopy(config)  # type: ignore

    if "filepath" not in _config:
        raise ConfigError(
            f"df_reader config must provide a 'filepath' property: {_config}"
        )

    if "engine" not in _config:
        _config["engine"] = {
            "path": "lot3.df_reader.reader.OasisPandasReader",
            "options": {},
        }
    elif isinstance(_config.get("engine"), str):
        _config["engine"] = {"path": _config.get("engine"), "options": {}}
    else:
        _config["engine"].setdefault("path", "lot3.df_reader.reader.OasisPandasReader")
        _config["engine"].setdefault("options", {})

    return _config  # type: ignore


def get_df_reader(config, *args, **kwargs):
    config = clean_config(config)

    cls = load_class(config["engine"]["path"], OasisReader)

    storage = config["engine"]["options"].pop("storage", None) or LocalStorageConnector(
        "/"
    )
    return cls(
        config["filepath"], storage, *args, **kwargs, **config["engine"]["options"]
    )
