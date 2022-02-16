import json

from .s3_model import S3Model


class S3Hashmap(S3Model):
    def __init__(self, *, model, log):
        super().__init__(model, log)
        self._key = model["key"]
        self._log = log.bind(
            client="s3_hashmap",
            key=self._key,
            bucket=self._bucket,
            model_name=self.model_name,
        )

    def _load_model(self):
        if self._res_type == "application/json":
            self._model_data = self._read_json()
        else:
            raise Exception("Unknown res type")

    def _read_json(self):
        res = self._s3_client.get_object(Bucket=self._bucket, Key=self._key)
        body = res["Body"].read().decode("utf-8")
        return json.loads(body)

    def invoke(self, key):
        self._log.debug("Invoke: Start")
        if self._model_data is None:
            self._log.info("Invoke: Load")
            self._load_model()
        return self._model_data.get(key, None)
