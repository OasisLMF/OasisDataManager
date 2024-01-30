#!/usr/bin/env python

import h5py
import pandas as pd

from oasis_data_manager.complex.complex import Adjustment, FileStoreComplexData
from oasis_data_manager.df_reader.reader import OasisDaskReader
from oasis_data_manager.filestore.backends.local import LocalStorage


class AddColAdjustment(Adjustment):
    @classmethod
    def apply(cls, df):
        df["else"] = "test"
        return df


class FloodDataExample(FileStoreComplexData):
    # Note this file needs to be sourced. TODO - is this public? If not is there a public/file we can include?
    filename = "tropical_cyclone_10synth_tracks_150arcsec_rcp26_KNA_2080.hdf5"
    adjustments = [AddColAdjustment]
    sql = "SELECT * FROM table WHERE event_id > 3000"

    def to_dataframe(self, result) -> pd.DataFrame:
        result = h5py.File(result)

        df = pd.DataFrame(list(result["event_id"]), columns=["event_id"])
        for m in ["event_name", "date", "frequency", "orig"]:
            df[m] = list(result[m])
        df = df.reset_index()

        return df


if __name__ == "__main__":
    reader = OasisDaskReader
    test_storage = LocalStorage("/tmp")

    result = FloodDataExample(storage=test_storage).run()
    print(result.as_pandas())
