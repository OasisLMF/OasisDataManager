from tempfile import NamedTemporaryFile

import numpy as np
import pandas as pd
import pytest

pyarrow = pytest.importorskip("pyarrow")

from oasis_data_manager.df_reader.backends.pyarrow import OasisPyarrowReader  # noqa: E402
from oasis_data_manager.filestore.backends.local import LocalStorage  # noqa: E402

storage = LocalStorage("/")


@pytest.fixture
def df():
    return pd.DataFrame(
        {
            "A": 1.0,
            "B": np.array([3, 3, 3, 3]),
            "C": ["test", "train", "test", "train"],
            "D": [10, 20, 30, 40],
        }
    )


def test_read_parquet__basic(df):
    with NamedTemporaryFile(suffix=".parquet") as parquet:
        df.to_parquet(parquet.name, index=False)

        result = OasisPyarrowReader(parquet.name, storage).as_pandas()

        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["A", "B", "C", "D"]
        assert len(result) == 4


def test_read_parquet__equality_filter(df):
    with NamedTemporaryFile(suffix=".parquet") as parquet:
        df.to_parquet(parquet.name, index=False)

        result = OasisPyarrowReader(
            parquet.name, storage, filters=[("C", "==", "test")]
        ).as_pandas()

        assert isinstance(result, pd.DataFrame)
        assert list(result["C"]) == ["test", "test"]
        assert len(result) == 2


def test_read_parquet__not_equal_filter(df):
    with NamedTemporaryFile(suffix=".parquet") as parquet:
        df.to_parquet(parquet.name, index=False)

        result = OasisPyarrowReader(
            parquet.name, storage, filters=[("C", "!=", "test")]
        ).as_pandas()

        assert isinstance(result, pd.DataFrame)
        assert list(result["C"]) == ["train", "train"]


def test_read_parquet__in_filter(df):
    with NamedTemporaryFile(suffix=".parquet") as parquet:
        df.to_parquet(parquet.name, index=False)

        result = OasisPyarrowReader(
            parquet.name, storage, filters=[("D", "in", [10, 30])]
        ).as_pandas()

        assert isinstance(result, pd.DataFrame)
        assert sorted(result["D"].tolist()) == [10, 30]


def test_read_parquet__not_in_filter(df):
    with NamedTemporaryFile(suffix=".parquet") as parquet:
        df.to_parquet(parquet.name, index=False)

        result = OasisPyarrowReader(
            parquet.name, storage, filters=[("D", "not in", [10, 30])]
        ).as_pandas()

        assert isinstance(result, pd.DataFrame)
        assert sorted(result["D"].tolist()) == [20, 40]


def test_read_parquet__compound_filter_and(df):
    """List of tuples → AND semantics."""
    with NamedTemporaryFile(suffix=".parquet") as parquet:
        df.to_parquet(parquet.name, index=False)

        result = OasisPyarrowReader(
            parquet.name,
            storage,
            filters=[("C", "==", "test"), ("D", ">", 15)],
        ).as_pandas()

        assert isinstance(result, pd.DataFrame)
        # Only row with C=="test" AND D>15 → row index 2 (D=30)
        assert len(result) == 1
        assert result.iloc[0]["D"] == 30


def test_read_parquet__compound_filter_or(df):
    """List of lists → OR semantics."""
    with NamedTemporaryFile(suffix=".parquet") as parquet:
        df.to_parquet(parquet.name, index=False)

        result = OasisPyarrowReader(
            parquet.name,
            storage,
            filters=[[("C", "==", "test")], [("D", "==", 20)]],
        ).as_pandas()

        assert isinstance(result, pd.DataFrame)
        # C=="test" gives rows 0,2; D==20 gives row 1 → 3 rows total
        assert len(result) == 3


def test_read_parquet__gt_lt_filters(df):
    with NamedTemporaryFile(suffix=".parquet") as parquet:
        df.to_parquet(parquet.name, index=False)

        result = OasisPyarrowReader(
            parquet.name, storage, filters=[("D", ">=", 20), ("D", "<=", 30)]
        ).as_pandas()

        assert isinstance(result, pd.DataFrame)
        assert sorted(result["D"].tolist()) == [20, 30]


def test_read_parquet__filter_mixed_types_raises(df):
    with NamedTemporaryFile(suffix=".parquet") as parquet:
        df.to_parquet(parquet.name, index=False)

        with pytest.raises(ValueError, match="Mixing and matching"):
            OasisPyarrowReader(
                parquet.name,
                storage,
                filters=[("C", "==", "test"), ["D", "==", 10]],
            ).as_pandas()
