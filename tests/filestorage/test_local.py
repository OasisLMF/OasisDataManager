import nturl2path
from pathlib import PurePosixPath, PureWindowsPath
from tempfile import TemporaryDirectory
from urllib.parse import urlparse

import pandas as pd

from oasis_data_manager.filestore.backends.local import LocalStorage, file_url_to_path, path_to_file_url
from oasis_data_manager.filestore.config import get_storage_from_config


def test_no_root_dir_is_set___root_dir_is_root():
    storage = LocalStorage()

    assert storage.root_dir == ""


def test_storage_constructed_from_config_matches_initial():
    with TemporaryDirectory() as d:
        storage = LocalStorage(root_dir=d)

        result = get_storage_from_config(storage.to_config())

        assert storage.root_dir == d
        assert isinstance(result, LocalStorage)
        assert result.root_dir == d


def test_file_url_of_windows_path_has_the_drive_in_its_path():
    url = path_to_file_url(PureWindowsPath(r"C:\Users\runner\data\location.csv"))

    assert url == "file:///C:/Users/runner/data/location.csv"
    assert urlparse(url).netloc == ""
    assert nturl2path.url2pathname(urlparse(url).path) == r"C:\Users\runner\data\location.csv"
    assert file_url_to_path(url) == "C:/Users/runner/data/location.csv"


def test_file_url_of_posix_path_is_unchanged():
    url = path_to_file_url(PurePosixPath("/data/location.csv"))

    assert url == "file:///data/location.csv"
    assert file_url_to_path(url) == "/data/location.csv"


def test_storage_url_is_absolute_and_readable(tmp_path, monkeypatch):
    (tmp_path / "location.csv").write_text("LocNumber,BuildingTIV\n1,100\n")
    monkeypatch.chdir(tmp_path)

    for storage in (LocalStorage(), LocalStorage(root_dir=str(tmp_path))):
        _, url = storage.get_storage_url("location.csv")

        assert url == f"file://{tmp_path / 'location.csv'}"
        assert pd.read_csv(url)["BuildingTIV"].tolist() == [100]
