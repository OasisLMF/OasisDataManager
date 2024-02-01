import logging
import pathlib
from io import BytesIO
from typing import List, Type

import httpcore
import httpx
import pandas as pd

from oasis_data_manager.df_reader.config import InputReaderConfig, clean_config, get_df_reader
from oasis_data_manager.df_reader.reader import OasisReader
from oasis_data_manager.filestore.backends.local import LocalStorage

logger = logging.getLogger(__name__)


class Adjustment:
    """
    Adjustments are any Pandas adjustments made after the data is fetched and filtered by SQL if applicable.
    """

    @classmethod
    def apply(cls, df):
        return df


class ComplexData:
    adjustments: List[Type[Adjustment]] = []
    filename: str = ""
    url: str = ""
    fetch_required: bool = True
    sql: str = ""

    def __init__(self, storage=None):
        if not storage:
            storage = LocalStorage()
        self.storage = storage

    def fetch(self):
        raise NotImplementedError

    def get_sql(self):
        return self.sql

    def adjust(self, reader) -> OasisReader:
        """
        Hook to apply any adjustments.

        TODO adjustments are filters? Functions fun on the readers df i.e
        apply, should be change filter to apply then or is that confusing with pandas?
        """
        if self.adjustments:
            return reader.filter([a.apply for a in self.adjustments])
        return reader

    def to_dataframe(self, result) -> pd.DataFrame:
        """
        Hook to allow conversion of the fetch() to a dataframe, in the case of the
        RestComplexData class for example, the result is json, so can be fed
        directly to a dataframe and wrapped into our df_reader.
        """
        return pd.DataFrame(result)

    def get_df_reader(self, filepath, **kwargs) -> OasisReader:
        df_reader_config = clean_config(
            InputReaderConfig(
                filepath=filepath,
                engine="oasis_data_manager.df_reader.reader.OasisDaskReader",
            )
        )
        df_reader_config["engine"]["options"]["storage"] = self.storage

        return get_df_reader(df_reader_config, **kwargs)

    def to_reader(self, fetch_result) -> OasisReader:
        if fetch_result:
            # When the result has been fetched, apply to_dataframe and pass directly into the df_reader
            df = self.to_dataframe(fetch_result)
            return self.get_df_reader(
                None,  # TODO - None value instead of filename_or_buffer, improve this.
                dataframe=df,
                has_read=True,
            )
        else:
            # Not fetched, let df_reader fetch as normal.
            return self.get_df_reader(self.filename if self.filename else self.url)

    def run(self):
        if self.fetch_required and self.filename:
            filename_or_url = self.filename if self.filename else self.url
            extension = pathlib.Path(filename_or_url).suffix
            self.fetch_required = extension not in [".parquet", ".pq", ".csv"]

        fetch_result = None
        if self.fetch_required:
            fetch_result = self.fetch()

        reader = self.to_reader(fetch_result)

        sql = self.get_sql()
        if hasattr(reader, "sql") and sql:
            reader = reader.sql(sql)

        reader = self.adjust(reader)

        # TODO store file? return for now
        return reader


class FileStoreComplexData(ComplexData):
    def to_dataframe(self, result) -> pd.DataFrame:
        """
        As this is only called on filetypes not handled by the df_reader, this will always be custom.
        """
        raise NotImplementedError

    def fetch(self):
        with self.storage.open(self.filename, "rb") as f:
            result = BytesIO(f.read())
        return result


class RestComplexData(ComplexData):
    exceptions = (
        httpx.RequestError,
        httpx.TimeoutException,
        httpx.ReadTimeout,
        httpx.ConnectTimeout,
        httpx.ConnectError,
        httpcore.ReadTimeout,
        httpcore.ConnectTimeout,
        httpcore.ConnectError,
    )

    url: str
    timeout: int = 10

    def handle_error(self, exception, **kwargs):
        logger.warning(
            "Exception in complex data call",
            extra={"exception": exception, "uri": self.url},
        )
        return None

    def handle_response(self, response) -> dict:
        return response.json()

    def get_uri(self) -> str:
        return self.url

    def get_headers(self) -> dict:
        return {}

    def get_post_json(self) -> dict:
        return {}

    def fetch(self):
        uri = self.get_uri()
        headers = self.get_headers()
        post_json = self.get_post_json()
        timeout = self.timeout

        try:
            if post_json:
                response = httpx.post(
                    uri, json=post_json, headers=headers, timeout=timeout
                )
            else:
                response = httpx.get(uri, headers=headers, timeout=timeout)

            if response.status_code == 200:
                return self.handle_response(response)
            else:
                return self.handle_error(
                    exception="Unexpected status in complex data call",
                    post_json=post_json,
                )
        except self.exceptions as e:
            return self.handle_error(exception=str(e), post_json=post_json)
