import json
import pickle
from io import BytesIO
from io import StringIO

import boto3
import pandas as pd
import hdbscan


def create_ds_models(**kwargs):
    return DsModels(**kwargs)


def create_ds_model(*, model, log):
    model_type = model["type"]
    if model_type == "sagemaker_endpoint":
        return SagemakerEndpoint(model=model, log=log)
    if model_type == "s3_hashmap":
        return S3Hashmap(model=model, log=log)
    if model_type == "s3_dataframe":
        return S3Dataframe(model=model, log=log)
    if model_type == "s3_dataframe_set":
        return S3DataframeSet(model=model, log=log)
    if model_type == "s3_scikit_set":
        return S3ScikitSet(model=model, log=log)

    raise Exception(f"Unknown model type: {model_type}")


def find_matching_model(models, filter_dict):
    output = []
    for model in models:
        key = model.pop("key")
        if model == filter_dict:
            output.append(model)
        model["key"] = key  # MUST restore the key

    if len(output) > 1:
        raise Exception(
            f"Found {len(models)} matches in model set but expected only 1 to match filter {json.dumps(filter_dict)}"
        )
    if len(output) == 0:
        return None
    return output[0]


class DsModels:
    def __init__(self, *, models, log):
        self._models = models
        self._log = log

    def get_ds_model(self, ds_model_name, /):
        model_data = self._pick_model_version(ds_model_name)
        log = self._log.bind(ds_model=ds_model_name)
        log.info("Selected model version", model_meta=model_data["model_name"])
        log.debug("Model data:", model_data=model_data)
        return create_ds_model(model=model_data, log=log)

    def _pick_model_version(self, model_name):
        versions = self._models.get(model_name)
        if versions is None:
            raise Exception(f"No model named: {model_name}")
        if len(versions) == 0:
            raise Exception(f"No versions for model: {model_name}")

        return versions[0]


class SagemakerEndpoint:
    def __init__(self, *, model, log):
        self.model_name = model["model_name"]
        self.model_parameters = model.get("parameters", {})
        self.columns = self.model_parameters.get("columns", [])
        self._endpoint_name = model["endpoint_name"]
        self._content_type = model["req_type"]
        self._res_type = model["res_type"]
        self._res_key = model.get("res_key", "data")
        self._log = log.bind(
            client="sagemaker_endpoint",
            endpoint_name=self._endpoint_name,
            model_name=self.model_name,
        )

    def invoke(self, dataframe):
        self._log.info("Invoke: Start")
        runtime = boto3.client("runtime.sagemaker")
        final_data = self._format_data(dataframe)
        response = runtime.invoke_endpoint(
            EndpointName=self._endpoint_name,
            ContentType=self._content_type,
            Body=final_data,
        )

        return self._transform_sagemaker_output(response)

    def _format_data(self, dataframe):
        if self._content_type == "text/csv":
            final_data = dataframe[self.columns].to_csv(index=False, header=False)
        else:
            raise Exception(f"Unknown content type {self._content_type}")
        return final_data

    def _transform_sagemaker_output(self, response):
        if self._res_type == "application/json":
            results = json.loads(response["Body"].read().decode())
            return pd.DataFrame.from_dict(results.get(self._res_key))

        raise Exception(f"Unknown res type {self._res_type}")


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


class S3Dataframe(S3Model):
    def __init__(self, *, model, log):
        super().__init__(model, log)
        self._key = model["key"]
        self._log = log.bind(
            client="s3_dataframe",
            key=self._key,
            bucket=self._bucket,
            model_name=self.model_name,
        )

    def _load_model(self):
        if self._res_type == "text/csv":
            self._model_data = self._read_csv()
        else:
            raise Exception(f"Unknown res type {self._res_type}")

    def _read_csv(self):
        res = self._s3_client.get_object(Bucket=self._bucket, Key=self._key)
        body = res["Body"].read().decode("utf-8")
        return pd.read_csv(StringIO(body))

    def invoke(self):
        self._log.debug("Invoke: Start")
        if self._model_data is None:
            self._log.info("Invoke: Load")
            self._load_model()
        return self._model_data


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
