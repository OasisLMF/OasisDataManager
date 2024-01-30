import json
import os
from tempfile import TemporaryDirectory

import pandas as pd
import pytest

from oasis_data_manager.complex.complex import FileStoreComplexData
from tests.filestorage.test_general import (
    aws_s3_storage,
    azure_abfs_storage,
    local_storage,
    test_file_name,
)


class TestFileStoreComplexData(FileStoreComplexData):
    filename = test_file_name

    def to_dataframe(self, result) -> pd.DataFrame:
        return pd.DataFrame(json.loads(result))


storage_factories = [
    local_storage,
    azure_abfs_storage,
    aws_s3_storage,
]


test_content = '{"A": [1, 2, 3], "B": ["a", "b", "c"]}'


@pytest.mark.parametrize("storage_factory", storage_factories)
def test_fetch__success(storage_factory):
    with storage_factory() as storage, TemporaryDirectory() as data:
        with open(os.path.join(data, test_file_name), "w") as f:
            f.write(test_content)

        storage.put(os.path.join(data, test_file_name), test_file_name)

        instance = TestFileStoreComplexData()
        instance.storage = storage
        result = instance.fetch()

    if hasattr(result, "read"):
        result = result.read()

    assert result.decode() == test_content


@pytest.mark.parametrize("storage_factory", storage_factories)
def test_to_dataframe(storage_factory):
    df = TestFileStoreComplexData().to_dataframe(test_content)

    assert isinstance(df, pd.DataFrame)
