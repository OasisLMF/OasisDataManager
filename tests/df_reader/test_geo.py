from tempfile import NamedTemporaryFile

import geodatasets
import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

from oasis_data_manager.df_reader.reader import (
    OasisDaskReaderCSV,
    OasisDaskReaderParquet,
    OasisPandasReaderCSV,
    OasisPandasReaderParquet,
)
from oasis_data_manager.filestore.backends.local import LocalStorage

READERS = [
    OasisPandasReaderCSV,
    OasisPandasReaderParquet,
    OasisDaskReaderCSV,
    OasisDaskReaderParquet,
]


storage = LocalStorage("/")


@pytest.fixture
def df():
    """
    df representing data with a long/lat, here we pull some values from
    within the nybb dataset used in tests to create some values in
    out expected boroughs.
    """
    b = [int(x) for x in gpd.read_file(geodatasets.get_path("nybb")).total_bounds]

    return pd.DataFrame(
        [
            {"longitude": x, "latitude": y, "value1": x + y, "value2": x - y}
            for x, y in zip(
                range(b[0], b[2], int((b[2] - b[0]) / 8)),
                range(b[1], b[3], int((b[3] - b[1]) / 8)),
            )
        ]
    )


@pytest.mark.parametrize("reader", READERS)
def test_read__expected_pandas_dataframe(reader, df):
    suffix = ".csv" if "csv" in reader.__name__.lower() else ".parquet"
    with NamedTemporaryFile(suffix=suffix) as file:
        if suffix == ".csv":
            df.to_csv(file.name, index=False)
        else:
            df.to_parquet(path=file.name, index=False)

        result = (
            reader(file.name, storage)
            .apply_geo(geodatasets.get_path("nybb"))
            .as_pandas()
        )

        assert isinstance(result, pd.DataFrame)
        assert result.to_dict() == {
            "longitude": {1: 932450, 2: 951725, 5: 1009550, 6: 1028825},
            "latitude": {1: 139211, 2: 158301, 5: 215571, 6: 234661},
            "value1": {1: 1071661, 2: 1110026, 5: 1225121, 6: 1263486},
            "value2": {1: 793239, 2: 793424, 5: 793979, 6: 794164},
        }


@pytest.mark.parametrize("reader", READERS)
def test_read__expected_pandas_dataframe__drop_geo(reader, df):
    suffix = ".csv" if "csv" in reader.__name__.lower() else ".parquet"
    with NamedTemporaryFile(suffix=suffix) as file:
        if suffix == ".csv":
            df.to_csv(file.name, index=False)
        else:
            df.to_parquet(path=file.name, index=False)

        result = (
            reader(
                file.name,
                storage,
            )
            .apply_geo(geodatasets.get_path("nybb"), drop_geo=False)
            .as_pandas()
        )

        assert isinstance(result, pd.DataFrame)
        assert result.to_dict() == {
            "longitude": {1: 932450, 2: 951725, 5: 1009550, 6: 1028825},
            "latitude": {1: 139211, 2: 158301, 5: 215571, 6: 234661},
            "value1": {1: 1071661, 2: 1110026, 5: 1225121, 6: 1263486},
            "value2": {1: 793239, 2: 793424, 5: 793979, 6: 794164},
            "geometry": {
                1: Point(932450, 139211),
                2: Point(951725, 158301),
                5: Point(1009550, 215571),
                6: Point(1028825, 234661),
            },
            "index_right": {1: 0, 2: 0, 5: 1, 6: 4},
            "BoroCode": {1: 5, 2: 5, 5: 4, 6: 2},
            "BoroName": {
                1: "Staten Island",
                2: "Staten Island",
                5: "Queens",
                6: "Bronx",
            },
            "Shape_Leng": {
                1: 330470.010332,
                2: 330470.010332,
                5: 896344.047763,
                6: 464392.991824,
            },
            "Shape_Area": {
                1: 1623819823.81,
                2: 1623819823.81,
                5: 3045212795.2,
                6: 1186924686.49,
            },
        }


@pytest.mark.parametrize("reader", READERS)
def test_read__gql_filter__expected_pandas_dataframe(reader, df):
    suffix = ".csv" if "csv" in reader.__name__.lower() else ".parquet"
    with NamedTemporaryFile(suffix=suffix) as file:
        if suffix == ".csv":
            df.to_csv(file.name, index=False)
        else:
            df.to_parquet(path=file.name, index=False)

        result = (
            reader(file.name, storage)
            .apply_geo(geodatasets.get_path("nybb"))
            .filter([lambda x: x[x["longitude"] == 1028825]])
            .as_pandas()
        )

        assert isinstance(result, pd.DataFrame)
        assert result.to_dict() == {
            "longitude": {6: 1028825},
            "latitude": {6: 234661},
            "value1": {6: 1263486},
            "value2": {6: 794164},
        }


@pytest.mark.parametrize("reader", READERS)
def test_read__shape_file__invalid(reader, df, caplog):
    df = df.rename(columns={"longitude": "lon", "latitude": "lat"})

    suffix = ".csv" if "csv" in reader.__name__.lower() else ".parquet"
    with NamedTemporaryFile(suffix=suffix) as file:
        if suffix == ".csv":
            df.to_csv(file.name, index=False)
        else:
            df.to_parquet(path=file.name, index=False)

        result = (
            reader(
                file.name,
                storage,
            )
            .apply_geo(geodatasets.get_path("nybb"))
            .as_pandas()
        )

        assert result.empty
        assert caplog.messages == ["Invalid shape file provided"]
