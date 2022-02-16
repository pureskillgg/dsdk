import pickle

import hdbscan

from .find import find_matching_model
from .s3_model import S3Model


class S3ScikitSet(S3Model):
    def __init__(self, *, model, log):
        super().__init__(model, log)
        self._prefix = model["prefix"]
        self._extension = model["extension"]
        self._scikits = model["scikits"]
        self._model_type = model["model_type"]
        self._model_selected = False
        self._selected_key = None
        self._log = log.bind(
            client="s3_scikit_set",
            prefix=self._prefix,
            bucket=self._bucket,
            model_name=self.model_name,
        )

    @property
    def scikits(self):
        return self._scikits

    def _load_model(self):
        if self._res_type == "application/x-pickle":
            model = self._read_pickle()
        else:
            raise Exception(f"Unknown res_type {self._res_type}")
        return model

    def _read_pickle(self):
        s3_key = self._get_key(self._selected_key)
        my_pickle = pickle.loads(
            self._s3_client.get_object(Bucket=self._bucket, Key=s3_key)["Body"].read()
        )
        return my_pickle

    def _use_model(self, model, data):
        if self._model_type == "hdbscan":
            # pylint: disable=unused-variable
            test_labels, strengths = hdbscan.approximate_predict(model, data)
            self._model_data = test_labels
        else:
            raise Exception(f"Unknown model_type {self._model_type}")

    def select(self, filter_dict):
        if self._model_selected:
            raise Exception(
                "Cannot select two different models. Call get_ds_model again to use another model."
            )
        self._model_selected = True
        scikit = find_matching_model(self._scikits, filter_dict)
        if scikit is not None:
            self._selected_key = scikit["key"]

    def invoke(self, dataframe):
        self._log.debug("Invoke: Start")
        if not self._model_selected:
            raise Exception("You must call select before invoke")
        if self._selected_key is None:
            return None
        model = self._load_model()
        self._use_model(model, dataframe)
        return self._model_data
