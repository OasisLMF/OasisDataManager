import numpy as np
import pandas as pd
import pytest

from oasis_data_manager.df_reader.reader import OasisDaskReader, OasisPandasReader, OasisReader
from oasis_data_manager.filestore.backends.local import LocalStorage

READERS = [OasisDaskReader, OasisPandasReader]

storage = LocalStorage("/")


@pytest.fixture
def df():
    return pd.DataFrame(
        {
            "A": 1.0,
            "B": np.array([3] * 4),
            "C": pd.Categorical(["test", "train", "test", "train"]),
        }
    )


@pytest.mark.parametrize("reader", READERS)
def test_read_csv__expected_pandas_dataframe(reader, df):
    result = reader(None, storage, dataframe=df, has_read=True)

    pandas_df = result.as_pandas()
    base = OasisReader.__class__
    assert pandas_df.__class__ is not base and base not in pandas_df.__class__.__bases__
    assert isinstance(pandas_df, pd.DataFrame)
    assert pandas_df.to_dict() == {
        "A": {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0},
        "B": {0: 3, 1: 3, 2: 3, 3: 3},
        "C": {0: "test", 1: "train", 2: "test", 3: "train"},
    }
