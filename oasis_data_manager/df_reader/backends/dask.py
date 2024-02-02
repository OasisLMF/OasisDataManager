import io
import pathlib
import logging

import dask
import dask_geopandas as dgpd
from dask import dataframe as dd
from dask_sql import Context
from dask_sql.utils import ParsingException
from distributed import Client

from ..exceptions import InvalidSQLException
from .base import OasisReader

dask.config.set(
    {"dataframe.convert-string": False}
)  # allows dask sql to support pyarrow
logger = logging.getLogger("oasis_data_manager.df_reader.reader")


class OasisDaskReader(OasisReader):
    sql_table_name = "table"

    def __init__(self, *args, client_address=None, **kwargs):
        if client_address:
            self.client = Client(client_address, set_as_default=False)
        else:
            self.client = None

        self.sql_context = Context()
        self.table_names = [self.sql_table_name]
        self.pre_sql_columns = []

        super().__init__(*args, **kwargs)

    def copy_with_df(self, df):
        res = super().copy_with_df(df)
        res.client = self.client
        return res

    def apply_geo(self, shape_filename_path, *args, drop_geo=True, **kwargs):
        """
        Read in a shape file and return the _read file with geo data joined.
        """
        # TODO: fix this so that it can work with non local files
        # with self.storage.open(self.shape_filename_path) as f:
        #     shape_df = dgpd.read_file(f, npartitions=1)

        shape_df = dgpd.read_file(shape_filename_path, npartitions=1)

        # for situations where the columns in the source data are different.
        lon_col = kwargs.get("geo_lon_col", "longitude")
        lat_col = kwargs.get("geo_lat_col", "latitude")

        df_columns = self.df.columns.tolist()
        if lat_col not in df_columns or lon_col not in df_columns:
            logger.warning("Invalid shape file provided")
            # temp until we decide on handling, i.e don't return full data if it fails.
            return self.copy_with_df(dd.DataFrame.from_dict({}, npartitions=1))

        df = self.df.copy()

        # convert read df to geo
        df["geometry"] = dgpd.points_from_xy(df, lon_col, lat_col)
        df = dgpd.from_dask_dataframe(df)

        # Make sure they're using the same projection reference
        df.crs = shape_df.crs

        # join the datasets, matching `geometry` to points within the shape df
        df = df.sjoin(shape_df, how="inner")

        if drop_geo:
            df = df.drop(shape_df.columns.tolist() + ["index_right"], axis=1)

        return self.copy_with_df(df)

    def apply_sql(self, sql):
        df = self.df.copy()
        try:
            # Initially this was the filename, but some filenames are invalid for the table,
            # is it ok to call it the same name all the time? Mapped to DaskDataTable in case
            # we need to change this.
            self.sql_context.create_table("DaskDataTable", self.df)
            formatted_sql = sql.replace(self.sql_table_name, "DaskDataTable")

            self.pre_sql_columns.extend(df.columns)

            # dask expects the columns to be lower case, which won't match some data
            df = self.sql_context.sql(
                formatted_sql,
                config_options={"sql.identifier.case_sensitive": False},
            )
            # which means we then need to map the columns back to the original
            # and allow for any aggregations to be retained
            validated_columns = []
            for v in df.columns:
                pre = False
                for x in self.pre_sql_columns:
                    if v.lower() == x.lower():
                        validated_columns.append(x)
                        pre = True

                if not pre:
                    validated_columns.append(v)
            df.columns = validated_columns

            return self.copy_with_df(df)
        except ParsingException:
            raise InvalidSQLException

    def join(self, df, table_name):
        """
        Creates a secondary table as a sql table in order to allow joins when apply_sql is called.
        """
        if table_name in self.table_names:
            raise RuntimeError(
                f"Table name already in use: [{','.join(self.table_names)}]"
            )
        self.pre_sql_columns.extend(df.columns)
        self.sql_context.create_table(table_name, df)
        self.table_names.append(table_name)
        return self

    def read_from_dataframe(self):
        if not isinstance(self.df, dd.DataFrame):
            self.df = dd.from_pandas(self.df, npartitions=1)

    def as_pandas(self):
        super().as_pandas()
        if self.client:
            return self.client.compute(self.df).result()
        else:
            return self.df.compute()

    def read_dict(self, data):
        self.df = dd.DataFrame.from_dict(data)

    def read_csv(self, *args, **kwargs):
        # remove standard pandas kwargs which will case an issue in dask.
        dask_safe_kwargs = kwargs.copy()
        dask_safe_kwargs.pop("memory_map", None)
        dask_safe_kwargs.pop("low_memory", None)

        filename_or_buffer = self.filename_or_buffer
        if isinstance(filename_or_buffer, pathlib.PosixPath):
            filename_or_buffer = str(self.filename_or_buffer)

        if isinstance(filename_or_buffer, io.TextIOWrapper) or isinstance(
            filename_or_buffer, io.BufferedReader
        ):
            filename_or_buffer = filename_or_buffer.name

        # django files
        if hasattr(filename_or_buffer, "path"):
            filename_or_buffer = filename_or_buffer.path

        _, uri = self.storage.get_storage_url(filename_or_buffer, encode_params=False)
        self.df = dd.read_csv(
            uri,
            *args,
            **dask_safe_kwargs,
            storage_options=self.storage.get_fsspec_storage_options(),
        )

    def read_parquet(self, *args, **kwargs):
        if isinstance(self.filename_or_buffer, str):
            _, uri = self.storage.get_storage_url(
                self.filename_or_buffer, encode_params=False
            )
            filename = uri
            kwargs["storage_options"] = self.storage.get_fsspec_storage_options()
        else:
            filename = self.filename_or_buffer

        self.df = dd.read_parquet(
            filename,
            *args,
            **kwargs,
        )

        # dask-sql doesn't handle categorical columns, but we need to be careful
        # how we convert them, if an assign is used we will end up stopping
        # the `Predicate pushdown optimization` within dask-sql from applying the
        # sql to the read_parquet filters.
        categories_to_convert = {}
        for col in self.df.select_dtypes(include="category").columns:
            categories_to_convert[col] = self.df[col].dtype.categories.dtype
        self.df = self.df.astype(categories_to_convert)


class OasisDaskReaderCSV(OasisDaskReader):
    pass


class OasisDaskReaderParquet(OasisDaskReader):
    pass
