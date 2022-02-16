import boto3


class S3Model:
    def __init__(self, model, log):
        self.model_name = model["model_name"]
        self.model_parameters = model.get("parameters", {})
        self._bucket = model["bucket"]
        self._res_type = model["res_type"]
        self._model_data = None
        self._s3_client = boto3.client("s3")
        self._prefix = ""
        self._extension = ""
        self._log = log

    def _get_key(self, key):
        return "/".join([self._prefix, ".".join([key, self._extension])])
