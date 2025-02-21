import pathlib
from typing import Iterable

from ...filestore.backends.base import BaseStorage


class OasisReader:
    """
    Base reader.

    as_pandas(), sql() & filter() can all be chained with self.has_read controlling whether the base
    read (read_csv/read_parquet) needs to be triggered. This is because in the case of spark
    we need to read differently depending on if the intention is to do sql or filter.
    """

    def __init__(
        self,
        filename_or_buffer,
        storage: BaseStorage,
        *args,
        dataframe=None,
        has_read=False,
        **kwargs,
    ):
        self.filename_or_buffer = filename_or_buffer
        self.storage = storage
        self._df = dataframe
        self.has_read = has_read
        self.reader_args = args
        self.reader_kwargs = kwargs

        if not filename_or_buffer:
            if dataframe is None and not has_read:
                raise RuntimeError(
                    "Reader must be initialised with either a "
                    "filename_or_buffer or by passing a dataframe "
                    "and has_read=True"
                )
            else:
                self.read_from_dataframe()

        if (
            filename_or_buffer
            and isinstance(self.filename_or_buffer, str)
            and self.filename_or_buffer.lower().endswith(".zip")
        ):
            self.reader_kwargs["compression"] = "zip"

    @property
    def df(self):
        self._read()
        return self._df

    @df.setter
    def df(self, other):
        self._df = other

    def read_csv(self, *args, **kwargs):
        raise NotImplementedError()

    def read_parquet(self, *args, **kwargs):
        raise NotImplementedError()

    def _read(self):
        if not self.has_read:
            if hasattr(self.filename_or_buffer, "name"):
                parts = pathlib.Path(self.filename_or_buffer.name).parts
            else:
                parts = pathlib.Path(self.filename_or_buffer).parts

            for part in parts:
                for extension in [".parquet", ".pq"]:
                    if part.endswith(extension):
                        is_parquet = True
                        break
                else:
                    continue  # if parquet extension is found the outer break will be called exiting the for with is_parquet = True
                break
            else:
                is_parquet = False

            if is_parquet:
                self.has_read = True
                self.read_parquet(*self.reader_args, **self.reader_kwargs)
            else:
                # assume the file is csv if not parquet
                self.has_read = True
                self.read_csv(*self.reader_args, **self.reader_kwargs)

        return self

    def copy_with_df(self, df):
        return type(self)(
            self.filename_or_buffer, self.storage, dataframe=df, has_read=self.has_read
        )

    def filter(self, filters):
        self._read()

        df = self.df
        for df_filter in filters if isinstance(filters, Iterable) else [filters]:
            df = df_filter(df)

        return self.copy_with_df(df)

    def sql(self, sql):
        if sql:
            self._read()
            return self.apply_sql(sql)
        return self

    def query(self, fn):
        return fn(self.df)

    def as_pandas(self):
        self._read()
        return self.df

    def read_from_dataframe(self):
        pass
