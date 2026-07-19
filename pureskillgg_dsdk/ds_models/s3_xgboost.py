from .s3_model import S3Model


class S3Xgboost(S3Model):
    def __init__(self, *, model, log):
        super().__init__(model, log)
        self._model_type = model["model_type"]
        self._key = model["key"]
        self._loaded_model = None
        self._log = log.bind(
            client="s3_xgboost",
            key=self._key,
            bucket=self._bucket,
            model_name=self.model_name,
        )

    def _load_model(self):
        if self._res_type == "application/json":
            model = self._read_json_model()
            return model
        raise Exception(f"Unknown res_type {self._res_type}")

    def _read_json_model(self):
        xgboost = self._import_xgboost()
        body = self._s3_client.get_object(Bucket=self._bucket, Key=self._key)[
            "Body"
        ].read()
        if self._model_type == "Booster":
            model = xgboost.Booster()
        else:
            model = xgboost.XGBClassifier()
        model.load_model(bytearray(body))
        return model

    def _use_model(self, dataframe):
        if self._model_type == "XGBClassifier":
            probabilities = self._loaded_model.predict_proba(dataframe)
            return probabilities
        if self._model_type == "Booster":
            xgboost = self._import_xgboost()
            dmatrix = xgboost.DMatrix(dataframe, enable_categorical=True)
            return self._loaded_model.predict(dmatrix)
        raise Exception(f"Unknown model_type {self._model_type}")

    @staticmethod
    def _import_xgboost():
        # pylint: disable=import-outside-toplevel
        try:
            import xgboost
        except ImportError as error:
            raise Exception(
                "xgboost is not installed: install the pureskillgg-dsdk[xgboost] extra"
            ) from error
        return xgboost

    def invoke(self, dataframe):
        self._log.debug("Invoke: Start")
        if self._loaded_model is None:
            self._loaded_model = self._load_model()
        probabilities = self._use_model(dataframe)
        return probabilities
