from tempfile import TemporaryDirectory

from oasis_data_manager.filestore.backends.local import LocalStorage
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
