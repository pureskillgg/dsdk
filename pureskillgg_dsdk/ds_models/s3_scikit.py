import pickle

from .s3_model import S3Model


class S3Scikit(S3Model):
    def __init__(self, *, model, log):
        super().__init__(model, log)
        self._model_type = model["model_type"]
        self._key = model["key"]
        self._loaded_model = None
        self._log = log.bind(
            client="s3_scikit",
            key=self._key,
            bucket=self._bucket,
            model_name=self.model_name,
        )

    def _load_model(self):
        if self._res_type == "application/x-pickle":
            model = self._read_pickle()
            return model
        raise Exception(f"Unknown res_type {self._res_type}")

    def _read_pickle(self):
        my_pickle = pickle.loads(
            self._s3_client.get_object(Bucket=self._bucket, Key=self._key)[
                "Body"
            ].read()
        )
        return my_pickle

    def _use_model(self, dataframe):
        if self._model_type == "MiniBatchKMeans":
            labels = self._loaded_model.predict(dataframe)
            return labels
        raise Exception(f"Unknown model_type {self._model_type}")

    def invoke(self, dataframe):
        self._log.debug("Invoke: Start")
        if self._loaded_model is None:
            self._loaded_model = self._load_model()
        labels = self._use_model(dataframe)
        return labels
