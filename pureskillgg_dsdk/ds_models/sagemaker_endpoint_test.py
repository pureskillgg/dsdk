# pylint: disable=missing-docstring,invalid-name

import io
import json

import pandas as pd
from structlog import get_logger

from . import sagemaker_endpoint
from .model import create_ds_models

MODELS = {
    "endpoint_test": [
        {
            "type": "sagemaker_endpoint",
            "model_name": "endpoint_test_v1",
            "endpoint_name": "test-endpoint",
            "req_type": "text/csv",
            "res_type": "application/json",
            "parameters": {"columns": ["a", "b"]},
        }
    ]
}


class StubRuntime:
    def __init__(self):
        self.endpoints = []

    def invoke_endpoint(self, EndpointName, ContentType, Body):
        assert ContentType == "text/csv"
        assert isinstance(Body, str)
        self.endpoints.append(EndpointName)
        payload = {"data": {"score": [0.25, 0.75]}}
        return {"Body": io.BytesIO(json.dumps(payload).encode("utf-8"))}


class StubBoto3:
    """Stands in for the boto3 module so no AWS client is ever built."""

    def __init__(self):
        self.services = []
        self.runtime = StubRuntime()

    def client(self, service_name):
        self.services.append(service_name)
        return self.runtime


def test_sagemaker_endpoint_creates_client_once(monkeypatch):
    stub_boto3 = StubBoto3()
    monkeypatch.setattr(sagemaker_endpoint, "boto3", stub_boto3)
    models = create_ds_models(models=MODELS, log=get_logger())
    ds_model = models.get_ds_model("endpoint_test")
    assert not stub_boto3.services

    frame = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    expected = pd.DataFrame({"score": [0.25, 0.75]})
    pd.testing.assert_frame_equal(ds_model.invoke(frame), expected)
    pd.testing.assert_frame_equal(ds_model.invoke(frame), expected)

    assert stub_boto3.services == ["runtime.sagemaker"]
    assert stub_boto3.runtime.endpoints == ["test-endpoint", "test-endpoint"]
