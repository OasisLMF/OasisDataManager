from tempfile import NamedTemporaryFile

import numpy as np
import pandas as pd
import pytest

from oasis_data_manager.df_reader.exceptions import InvalidSQLException
from oasis_data_manager.df_reader.reader import OasisDaskReaderParquet, OasisPandasReaderParquet
from oasis_data_manager.filestore.backends.local import LocalStorage

READERS = [OasisPandasReaderParquet, OasisDaskReaderParquet]

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
def test_read_parquet__expected_pandas_dataframe(reader, df):
    with NamedTemporaryFile(suffix=".parquet") as parquet:
        df.to_parquet(path=parquet.name, index=False)

        result = reader(parquet.name, storage).as_pandas()

        assert isinstance(result, pd.DataFrame)
        assert result.to_dict() == {
            "A": {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0},
            "B": {
                0: pd.Timestamp("20230101"),
                1: pd.Timestamp("20230102"),
                2: pd.Timestamp("20230102"),
                3: pd.Timestamp("20230102"),
            },
            "C": {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0},
            "D": {0: 3, 1: 3, 2: 3, 3: 3},
            "E": {0: "test", 1: "train", 2: "test", 3: "train"},
            "F": {0: "foo", 1: "foo", 2: "foo", 3: "foo"},
        }


@pytest.mark.parametrize("reader", READERS)
def test_read_parquet__df_filter__expected_pandas_dataframe(reader, df):
    with NamedTemporaryFile(suffix=".parquet") as parquet:
        df.to_parquet(path=parquet.name, index=False)

        def sample_filter(filter_df):
            return filter_df[filter_df["E"] == "test"]

        result = reader(parquet.name, storage).filter([sample_filter]).as_pandas()

        assert isinstance(result, pd.DataFrame)
        assert result.to_dict() == {
            "A": {0: 1.0, 2: 1.0},
            "B": {0: pd.Timestamp("20230101"), 2: pd.Timestamp("20230102")},
            "C": {0: 1.0, 2: 1.0},
            "D": {0: 3, 2: 3},
            "E": {0: "test", 2: "test"},
            "F": {0: "foo", 2: "foo"},
        }


@pytest.mark.parametrize("reader", READERS)
def test_read_parquet__df_filter__multiple__expected_pandas_dataframe(reader, df):
    with NamedTemporaryFile(suffix=".parquet") as parquet:
        df.to_parquet(path=parquet.name, index=False)

        result = (
            reader(parquet.name, storage)
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
            "B": {2: pd.Timestamp("20230102")},
            "C": {2: 1.0},
            "D": {2: 3},
            "E": {2: "test"},
            "F": {2: "foo"},
        }


def test_read_parquet__dask__sql__expected_pandas_dataframe(df):
    with NamedTemporaryFile(suffix=".parquet") as parquet:
        df.to_parquet(path=parquet.name, index=False)

        result = (
            OasisDaskReaderParquet(parquet.name, storage)
            .sql("SELECT * FROM table WHERE E = 'test' AND B = '2023-01-02'")
            .as_pandas()
        )

        assert isinstance(result, pd.DataFrame)
        assert result.to_dict() == {
            "A": {0: 1.0},
            "B": {0: pd.Timestamp("20230102")},
            "C": {0: 1.0},
            "D": {0: 3},
            "E": {0: "test"},
            "F": {0: "foo"},
        }


def test_read_parquet__dask__sql__invalid_sql(df):
    with NamedTemporaryFile(suffix=".parquet") as parquet:
        df.to_parquet(path=parquet.name, index=False)

        with pytest.raises(InvalidSQLException):
            OasisDaskReaderParquet(parquet.name, storage).sql(
                "SELECT X FROM table"
            ).as_pandas()


def test_read_parquet__dask__sql__no_data(df):
    with NamedTemporaryFile(suffix=".parquet") as parquet:
        df.to_parquet(path=parquet.name, index=False)

        result = (
            OasisDaskReaderParquet(parquet.name, storage)
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
