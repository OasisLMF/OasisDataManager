from unittest.mock import patch

import pandas as pd
import pytest
from dask import dataframe as dd

from oasis_data_manager.complex.complex import Adjustment, ComplexData
from oasis_data_manager.df_reader.reader import OasisReader
# from oasis_data_manager.filestore.backends.local import LocalStorage


@pytest.fixture(autouse=True)
def patch_read_csv():
    with patch(
        "dask.dataframe.read_csv",
        return_value=dd.DataFrame.from_dict(
            {"A": [1, 2, 3], "B": ["a", "b", "c"]}, npartitions=1
        ),
    ):
        yield


class TestComplexData(ComplexData):
    url = "https://test.com"

    def fetch(self):
        return {"A": [1, 2, 3], "B": ["a", "b", "c"]}


class TestComplexDataFile(ComplexData):
    filename = "test.csv"

    def fetch(self):
        raise Exception("This won't be called.")


class TestComplexDataFetch(TestComplexData):
    fetch_required = True


COMPLEX_CLASSES = [TestComplexData, TestComplexDataFetch, TestComplexDataFile]


class AdjustmentA(Adjustment):
    @classmethod
    def apply(cls, df):
        df["A"] = df["A"].apply(lambda x: x * 2)
        return df


class AdjustmentB(Adjustment):
    @classmethod
    def apply(cls, df):
        df["B"] = df["B"].apply(lambda x: x + "...")
        return df


def adjustment(df):
    df["A"] = df["A"].apply(lambda x: x * 2)
    return df


def test_to_dataframe():
    result = {"A": [1, 2, 3], "B": ["a", "b", "c"]}
    df = ComplexData().to_dataframe(result)

    assert isinstance(df, pd.DataFrame)
    assert df.to_dict() == {
        "A": {0: 1.0, 1: 2.0, 2: 3.0},
        "B": {
            0: "a",
            1: "b",
            2: "c",
        },
    }


def test_dataframe__as_reader():
    result = {"A": [1, 2, 3], "B": ["a", "b", "c"]}
    instance = ComplexData()
    reader = instance.get_df_reader(
        None, dataframe=instance.to_dataframe(result), has_read=True
    )

    base = OasisReader.__class__
    assert reader.__class__ is not base and base not in reader.__class__.__bases__
    df = reader.as_pandas()
    assert isinstance(df, pd.DataFrame)
    assert df.to_dict() == {
        "A": {0: 1.0, 1: 2.0, 2: 3.0},
        "B": {
            0: "a",
            1: "b",
            2: "c",
        },
    }


@pytest.mark.parametrize("klass", COMPLEX_CLASSES)
def test_run(klass):
    result = klass().run()

    # TODO what's the outcome a stored file? for now check the df is correct
    assert result.as_pandas().to_dict() == {
        "A": {0: 1, 1: 2, 2: 3},
        "B": {0: "a", 1: "b", 2: "c"},
    }


@pytest.mark.parametrize("klass", COMPLEX_CLASSES)
@pytest.mark.parametrize("extension", [".parquet", ".pq", ".csv"])
def test_run__fetch_skipped(klass, extension):
    instance = klass()
    instance.url = f"https://test.com/test.{extension}"
    result = instance.run()

    # TODO what's the outcome a stored file? for now check the df is correct
    assert result.as_pandas().to_dict() == {
        "A": {0: 1, 1: 2, 2: 3},
        "B": {0: "a", 1: "b", 2: "c"},
    }


@pytest.mark.parametrize("klass", COMPLEX_CLASSES)
def test_run__adjust(klass):
    instance = klass()
    instance.adjustments = [AdjustmentA]
    result = instance.run()

    # TODO what's the outcome a stored file? for now check the df is correct
    assert result.as_pandas().to_dict() == {
        "A": {0: 2, 1: 4, 2: 6},
        "B": {0: "a", 1: "b", 2: "c"},
    }


@pytest.mark.parametrize("klass", COMPLEX_CLASSES)
def test_run__adjust__chained(klass):
    instance = klass()
    instance.adjustments = [AdjustmentA, AdjustmentB]
    result = instance.run()

    # TODO what's the outcome a stored file? for now check the df is correct
    assert result.as_pandas().to_dict() == {
        "A": {0: 2, 1: 4, 2: 6},
        "B": {0: "a...", 1: "b...", 2: "c..."},
    }
