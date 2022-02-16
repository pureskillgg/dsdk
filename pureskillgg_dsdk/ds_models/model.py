from .sagemaker_endpoint import SagemakerEndpoint
from .s3_hashmap import S3Hashmap
from .s3_dataframe import S3Dataframe
from .s3_dataframe_set import S3DataframeSet
from .s3_scikit import S3Scikit
from .s3_scikit_set import S3ScikitSet


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
    if model_type == "s3_scikit":
        return S3Scikit(model=model, log=log)

    raise Exception(f"Unknown model type: {model_type}")


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
