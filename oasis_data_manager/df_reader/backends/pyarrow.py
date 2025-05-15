import pyarrow.dataset as ds
from functools import reduce

from .base import OasisReader


class OasisPyarrowReader(OasisReader):

    def read_parquet(self, *args, **kwargs):
        def pyarrow_dataset_filter(pandas_filter):
            def pyarrow_dataset_single_filter(pandas_single_filter):
                op_map = {
                    "==": lambda x, y: ds.field(x) == y,
                    "!=": lambda x, y: ds.field(x) != y,
                    ">=": lambda x, y: ds.field(x) >= y,
                    "<=": lambda x, y: ds.field(x) <= y,
                    ">": lambda x, y: ds.field(x) > y,
                    "<": lambda x, y: ds.field(x) < y,
                    "in": lambda x, y: ds.field(x).isin(y),
                    "not in": lambda x, y: ~ds.field(x).isin(y)
                }

                field_name, operator, value = pandas_single_filter
                df_filter = op_map.get(operator, lambda x, y: None)(field_name, value)

                return df_filter

            def list_of_tuples(lot):
                # Apply pyarrow_dataset_single_filter to each tuple in the list and combine using &
                return reduce(lambda x, y: x & y, (pyarrow_dataset_single_filter(f) for f in lot))

            def list_of_lists(lol):
                # Convert each list in lol to a tuple of filters using list_of_tuples, then combine using |
                return reduce(lambda x, y: x | y, (list_of_tuples(f) for f in lol))

            # Check if all elements in pandas_filter are of the same type (either tuple or list)
            element_types = {type(f) for f in pandas_filter}
            if len(element_types) > 1:
                raise ValueError("Mixing and matching tuples and lists")

            # Process based on the single type found
            if isinstance(pandas_filter[0], tuple):
                new_filter = list_of_tuples(pandas_filter)
            elif isinstance(pandas_filter[0], list):
                new_filter = list_of_lists(pandas_filter)
            else:
                raise TypeError("Unsupported type in filter")

            return new_filter

        if 'filters' in kwargs:
            ds_filter = pyarrow_dataset_filter(kwargs['filters'])
        else:
            ds_filter = None

        if isinstance(self.filename_or_buffer, str):
            if self.filename_or_buffer.startswith(
                    "http://"
            ) or self.filename_or_buffer.startswith("https://"):
                dataset = ds.dataset(self.filename_or_buffer, partitioning='hive')
                self.df = dataset.to_table(filter=ds_filter).to_pandas()
            else:
                _, uri = self.storage.get_storage_url(
                    self.filename_or_buffer, encode_params=False
                )

                uri = uri.replace('file://', '')
                dataset = ds.dataset(uri, partitioning='hive')
                self.df = dataset.to_table(filter=ds_filter).to_pandas()

        else:
            dataset = ds.dataset(self.filename_or_buffer, partitioning='hive')
            self.df = dataset.to_table(filter=ds_filter).to_pandas()
