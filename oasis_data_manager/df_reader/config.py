import json
import sys
from copy import deepcopy
from pathlib import Path

if sys.version_info >= (3, 8):
    from typing import Any, Dict, TypedDict, Union
    from typing_extensions import NotRequired
else:
    from typing import Any, Dict, Union
    from typing_extensions import NotRequired, TypedDict

from ..config import ConfigError, load_class
from ..filestore.backends.local import LocalStorage
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

    return _config  # type: ignore


def get_df_reader(config, *args, **kwargs):
    config = clean_config(config)
    cls = load_class(config["engine"]["path"], OasisReader)
    storage = config["engine"]["options"].pop("storage", None) or LocalStorage("/")

    return cls(
        config["filepath"], storage, *args, **kwargs, **config["engine"]["options"]
    )
