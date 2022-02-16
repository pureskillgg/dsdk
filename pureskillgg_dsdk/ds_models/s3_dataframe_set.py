from io import BytesIO

import pandas as pd

from .find import find_matching_model
from .s3_model import S3Model


class S3DataframeSet(S3Model):
    def __init__(self, *, model, log):
        super().__init__(model, log)
        self._prefix = model["prefix"]
        self._extension = model["extension"]
        self._dataframes = model["dataframes"]
        self._model_selected = False
        self._selected_key = None
        self._log = log.bind(
            client="s3_dataframe_set",
            prefix=self._prefix,
            bucket=self._bucket,
            model_name=self.model_name,
        )

    @property
    def dataframes(self):
        return self._dataframes

    def _load_model(self):
        if self._res_type == "application/x-parquet":
            self._model_data = self._read_parquet()
        else:
            raise Exception("Unknown res_type {self._res_type}")

    def _read_parquet(self):
        s3_key = self._get_key(self._selected_key)
        obj = self._s3_client.get_object(Bucket=self._bucket, Key=s3_key)
        dataframe = pd.read_parquet(BytesIO(obj["Body"].read()))
        return dataframe

    def select(self, filter_dict):
        if self._model_selected:
            raise Exception(
                "Cannot select two different models. Call get_ds_model again to use another model."
            )
        self._model_selected = True
        dataframe = find_matching_model(self._dataframes, filter_dict)
        if dataframe is not None:
            self._selected_key = dataframe["key"]

    def invoke(self):
        self._log.debug("Invoke: Start")
        if not self._model_selected:
            raise Exception("You must call select before invoke")
        if self._selected_key is None:
            return None
        self._load_model()
        return self._model_data
