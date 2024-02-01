import logging

import pandas as pd


try:
    import geopandas as gpd
except ModuleNotFoundError:
    gpd = None

from .base import OasisReader
from ..exceptions import MissingOptionalDependency
logger = logging.getLogger("oasis_data_manager.df_reader.reader")


class OasisPandasReader(OasisReader):
    def read_csv(self, *args, **kwargs):
        if isinstance(self.filename_or_buffer, str):
            if self.filename_or_buffer.startswith(
                "http://"
            ) or self.filename_or_buffer.startswith("https://"):
                self.df = pd.read_csv(self.filename_or_buffer, *args, **kwargs)
            else:
                _, uri = self.storage.get_storage_url(
                    self.filename_or_buffer, encode_params=False
                )
                self.df = pd.read_csv(
                    uri,
                    *args,
                    **kwargs,
                    storage_options=self.storage.get_fsspec_storage_options(),
                )
        else:
            self.df = pd.read_csv(self.filename_or_buffer, *args, **kwargs)

    def read_parquet(self, *args, **kwargs):
        if isinstance(self.filename_or_buffer, str):
            if self.filename_or_buffer.startswith(
                "http://"
            ) or self.filename_or_buffer.startswith("https://"):
                self.df = pd.read_parquet(self.filename_or_buffer, *args, **kwargs)
            else:
                _, uri = self.storage.get_storage_url(
                    self.filename_or_buffer, encode_params=False
                )
                self.df = pd.read_parquet(
                    uri,
                    *args,
                    **kwargs,
                    storage_options=self.storage.get_fsspec_storage_options(),
                )
        else:
            self.df = pd.read_parquet(self.filename_or_buffer, *args, **kwargs)

    def apply_geo(self, shape_filename_path, *args, drop_geo=True, **kwargs):
        """
        Read in a shape file and return the _read file with geo data joined.
        """
        # TODO: fix this so that it can work with non local files
        # with self.storage.open(self.shape_filename_path) as f:
        #     shape_df = gpd.read_file(f)

        if gpd is None:
            raise MissingOptionalDependency(
                "Missing optional dependency 'geopandas' for 'apply_geo' method, install package using `pip install oasis-data-manager[extra]`")

        shape_df = gpd.read_file(shape_filename_path)

        # for situations where the columns in the source data are different.
        lon_col = kwargs.get("geo_lon_col", "longitude")
        lat_col = kwargs.get("geo_lat_col", "latitude")

        df_columns = self.df.columns.tolist()
        if lat_col not in df_columns or lon_col not in df_columns:
            logger.warning("Invalid shape file provided")
            # temp until we decide on handling, i.e don't return full data if it fails.
            return self.copy_with_df(pd.DataFrame.from_dict({}))

        # convert read df to geo
        df = gpd.GeoDataFrame(
            self.df, geometry=gpd.points_from_xy(self.df[lon_col], self.df[lat_col])
        )

        # Make sure they're using the same projection reference
        df.crs = shape_df.crs

        # join the datasets, matching `geometry` to points within the shape df
        df = df.sjoin(shape_df, how="inner")

        if drop_geo:
            df = df.drop(shape_df.columns.tolist() + ["index_right"], axis=1)

        return self.copy_with_df(df)


class OasisPandasReaderCSV(OasisPandasReader):
    pass


class OasisPandasReaderParquet(OasisPandasReader):
    pass
