# pylint: disable=missing-docstring,invalid-name,attribute-defined-outside-init,protected-access

import io
import os

from structlog import get_logger

from .model import create_ds_models

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

MODELS = {
    "xgb_test": [
        {
            "type": "s3_xgboost",
            "model_name": "xgb_test_v1",
            "model_type": "XGBClassifier",
            "bucket": "some-bucket",
            "key": "department/test/xgb_test/model_v1.json",
            "res_type": "application/json",
        }
    ]
}


class FakeClassifier:
    def load_model(self, buffer):
        self.buffer = bytes(buffer)

    def predict_proba(self, dataframe):
        return [[0.25, 0.75] for _ in dataframe]


class FakeXgboost:
    XGBClassifier = FakeClassifier


class FakeS3Client:
    def get_object(self, Bucket, Key):
        assert Bucket == "some-bucket"
        assert Key == "department/test/xgb_test/model_v1.json"
        return {"Body": io.BytesIO(b'{"fake": "model"}')}


def test_s3_xgboost_uses_injected_module():
    models = create_ds_models(models=MODELS, xgboost=FakeXgboost(), log=get_logger())
    ds_model = models.get_ds_model("xgb_test")
    ds_model._s3_client = FakeS3Client()
    assert ds_model.invoke([1, 2]) == [[0.25, 0.75], [0.25, 0.75]]
    assert ds_model._loaded_model.buffer == b'{"fake": "model"}'
