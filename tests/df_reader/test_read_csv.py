from tempfile import NamedTemporaryFile
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

try:
    import dask.dataframe as dd
except ModuleNotFoundError:
    dd = None

from oasis_data_manager.df_reader.exceptions import InvalidSQLException
from oasis_data_manager.df_reader.reader import OasisPandasReaderCSV
from oasis_data_manager.filestore.backends.local import LocalStorage

try:
    from oasis_data_manager.df_reader.reader import OasisDaskReaderCSV
except ImportError:
    OasisDaskReaderCSV = None  # type: ignore[assignment]

READERS = [r for r in [OasisPandasReaderCSV, OasisDaskReaderCSV] if r is not None]

storage = LocalStorage("/")


@pytest.fixture
def df():
    return pd.DataFrame(
        {
            "A": 1.0,
            "B": [
                pd.Timestamp("20230101"),
                pd.Timestamp("20230102"),
                pd.Timestamp("20230102"),
                pd.Timestamp("20230102"),
            ],
            "C": pd.Series(1, index=list(range(4)), dtype="float64"),
            "D": np.array([3] * 4),
            "E": pd.Categorical(["test", "train", "test", "train"]),
            "F": "foo",
        }
    )


@pytest.mark.parametrize("reader", READERS)
def test_read_csv__expected_pandas_dataframe(reader, df):
    with NamedTemporaryFile(suffix=".csv") as csv:
        df.to_csv(
            path_or_buf=csv.name, columns=df.columns, encoding="utf-8", index=False
        )

        result = reader(csv.name, storage).as_pandas()

        assert isinstance(result, pd.DataFrame)
        assert result.to_dict() == {
            "A": {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0},
            "B": {
                0: "2023-01-01",
                1: "2023-01-02",
                2: "2023-01-02",
                3: "2023-01-02",
            },
            "C": {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0},
            "D": {0: 3, 1: 3, 2: 3, 3: 3},
            "E": {0: "test", 1: "train", 2: "test", 3: "train"},
            "F": {0: "foo", 1: "foo", 2: "foo", 3: "foo"},
        }


@pytest.mark.parametrize("reader", READERS)
def test_read_csv__df_filter__expected_pandas_dataframe(reader, df):
    with NamedTemporaryFile(suffix=".csv") as csv:
        df.to_csv(
            path_or_buf=csv.name, columns=df.columns, encoding="utf-8", index=False
        )

        def sample_filter(filter_df):
            return filter_df[filter_df["E"] == "test"]

        result = reader(csv.name, storage).filter([sample_filter]).as_pandas()

        assert isinstance(result, pd.DataFrame)
        assert result.to_dict() == {
            "A": {0: 1.0, 2: 1.0},
            "B": {0: "2023-01-01", 2: "2023-01-02"},
            "C": {0: 1.0, 2: 1.0},
            "D": {0: 3, 2: 3},
            "E": {0: "test", 2: "test"},
            "F": {0: "foo", 2: "foo"},
        }


@pytest.mark.parametrize("reader", READERS)
def test_read_csv__df_filter__multiple__expected_pandas_dataframe(reader, df):
    with NamedTemporaryFile(suffix=".csv") as csv:
        df.to_csv(
            path_or_buf=csv.name, columns=df.columns, encoding="utf-8", index=False
        )

        result = (
            reader(csv.name, storage)
            .filter(
                [
                    lambda x: x[x["E"] == "test"],
                    lambda x: x[x["B"] == "2023-01-02"],
                ]
            )
            .as_pandas()
        )

        assert isinstance(result, pd.DataFrame)
        assert result.to_dict() == {
            "A": {2: 1.0},
            "B": {2: "2023-01-02"},
            "C": {2: 1.0},
            "D": {2: 3},
            "E": {2: "test"},
            "F": {2: "foo"},
        }


@pytest.mark.skipif(OasisDaskReaderCSV is None, reason="dask not installed")
def test_read_csv__dask__removes_bad_kwargs(df):
    with NamedTemporaryFile(suffix=".csv") as csv:
        df.to_csv(
            path_or_buf=csv.name, columns=df.columns, encoding="utf-8", index=False
        )

        with patch("dask.dataframe.read_csv") as dask_read_csv:
            OasisDaskReaderCSV(
                csv.name, storage, memory_map=True, low_memory=True, encoding="utf-8"
            ).as_pandas()

        assert len(dask_read_csv.call_args[0]) == 1
        assert dask_read_csv.call_args[0][0] == f"file://{csv.name}"
        assert len(dask_read_csv.call_args[1]) == 2
        assert dask_read_csv.call_args[1]["encoding"] == "utf-8"
        assert dask_read_csv.call_args[1]["storage_options"] == {}


@pytest.mark.skip(reason="Broken test")
def test_read_csv__dask__sql__expected_pandas_dataframe(df):
    with NamedTemporaryFile(suffix=".csv") as csv:
        df.to_csv(
            path_or_buf=csv.name, columns=df.columns, encoding="utf-8", index=False
        )

        result = (
            OasisDaskReaderCSV(csv.name, storage)
            .sql("SELECT * FROM table WHERE E = 'test' AND B = '2023-01-02'")
            .as_pandas()
        )

        assert isinstance(result, pd.DataFrame)
        assert result.to_dict() == {
            "A": {2: 1.0},
            "B": {2: "2023-01-02"},
            "C": {2: 1.0},
            "D": {2: 3},
            "E": {2: "test"},
            "F": {2: "foo"},
        }


@pytest.mark.skip(reason="Broken test")
def test_read_csv__dask__sql__invalid_sql(df):
    with NamedTemporaryFile(suffix=".csv") as csv:
        df.to_csv(
            path_or_buf=csv.name, columns=df.columns, encoding="utf-8", index=False
        )

        with pytest.raises(InvalidSQLException):
            OasisDaskReaderCSV(csv.name, storage).sql("SELECT X FROM table").as_pandas()


@pytest.mark.skip(reason="Broken test")
def test_read_csv__dask__sql__no_data(df):
    with NamedTemporaryFile(suffix=".csv") as csv:
        df.to_csv(
            path_or_buf=csv.name, columns=df.columns, encoding="utf-8", index=False
        )

        result = (
            OasisDaskReaderCSV(csv.name, storage)
            .sql("SELECT * FROM table WHERE E = 'tester'")
            .as_pandas()
        )

        assert isinstance(result, pd.DataFrame)
        assert result.to_dict() == {
            "A": {},
            "B": {},
            "C": {},
            "D": {},
            "E": {},
            "F": {},
        }


# ---------------------------------------------------------------------------
# OasisReader.query()
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("reader", READERS)
def test_query__returns_function_result(reader, df):
    with NamedTemporaryFile(suffix=".csv") as csv:
        df.to_csv(path_or_buf=csv.name, encoding="utf-8", index=False)

        result = reader(csv.name, storage).query(lambda frame: len(frame))

        assert result == 4


@pytest.mark.parametrize("reader", READERS)
def test_query__dataframe_transform(reader, df):
    with NamedTemporaryFile(suffix=".csv") as csv:
        df.to_csv(path_or_buf=csv.name, encoding="utf-8", index=False)

        result = reader(csv.name, storage).query(lambda frame: frame["D"].sum())

        # Dask returns a lazy scalar; compute it before comparing
        if hasattr(result, "compute"):
            result = result.compute()
        assert result == 12  # 4 rows × D=3


# ---------------------------------------------------------------------------
# OasisReader.copy_with_df()
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("reader", READERS)
def test_copy_with_df__new_reader_has_replacement_dataframe(reader, df):
    with NamedTemporaryFile(suffix=".csv") as csv:
        df.to_csv(path_or_buf=csv.name, encoding="utf-8", index=False)

        original = reader(csv.name, storage)
        # Trigger read first (copy_with_df is called post-read e.g. from filter())
        original._read()
        replacement = pd.DataFrame({"X": [1, 2, 3]})
        copy = original.copy_with_df(replacement)

        assert type(copy) is type(original)
        result = copy.as_pandas()
        # Dask returns a computed dask DataFrame; normalize to pandas
        if dd is not None and isinstance(result, dd.DataFrame):
            result = result.compute()
        assert list(result.columns) == ["X"]
        assert len(result) == 3


@pytest.mark.parametrize("reader", READERS)
def test_copy_with_df__original_reader_unchanged(reader, df):
    with NamedTemporaryFile(suffix=".csv") as csv:
        df.to_csv(path_or_buf=csv.name, encoding="utf-8", index=False)

        original = reader(csv.name, storage)
        _ = original.copy_with_df(pd.DataFrame({"X": [1]}))

        # original should still read the full CSV
        result = original.as_pandas()
        assert len(result) == 4


# ---------------------------------------------------------------------------
# OasisDaskReader.read_from_dataframe()
# ---------------------------------------------------------------------------

@pytest.mark.skipif(OasisDaskReaderCSV is None or dd is None, reason="dask not installed")
def test_dask_read_from_dataframe__converts_pandas_to_dask(df):
    """Passing a pandas df via dataframe= should be available as a dask DataFrame."""
    dask_reader = OasisDaskReaderCSV(None, storage, dataframe=df, has_read=True)

    assert isinstance(dask_reader._df, dd.DataFrame)
    result = dask_reader.as_pandas()
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 4
