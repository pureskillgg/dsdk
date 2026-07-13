# pylint: disable=missing-docstring,invalid-name,protected-access

import io
import os

import numpy as np
import xgboost
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


class FakeS3Client:
    def __init__(self, body):
        self._body = body

    def get_object(self, Bucket, Key):
        assert Bucket == "some-bucket"
        assert Key == "department/test/xgb_test/model_v1.json"
        return {"Body": io.BytesIO(self._body)}


def test_s3_xgboost_matches_local_predictions(tmp_path):
    rng = np.random.default_rng(0)
    features = rng.random((80, 4))
    target = (features[:, 0] > 0.5).astype(int)
    trained = xgboost.XGBClassifier(n_estimators=4, max_depth=2)
    trained.fit(features, target)
    artifact = tmp_path / "model_v1.json"
    trained.save_model(artifact)

    models = create_ds_models(models=MODELS, log=get_logger())
    ds_model = models.get_ds_model("xgb_test")
    ds_model._s3_client = FakeS3Client(artifact.read_bytes())

    probabilities = ds_model.invoke(features)
    np.testing.assert_array_equal(probabilities, trained.predict_proba(features))
