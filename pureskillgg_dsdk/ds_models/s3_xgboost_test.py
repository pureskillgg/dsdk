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
    ],
    "booster_test": [
        {
            "type": "s3_xgboost",
            "model_name": "booster_test_v1",
            "model_type": "Booster",
            "bucket": "some-bucket",
            "key": "department/test/booster_test/model_v1.json",
            "res_type": "application/json",
        }
    ],
}


class FakeS3Client:
    def __init__(self, body, key="department/test/xgb_test/model_v1.json"):
        self._body = body
        self._key = key

    def get_object(self, Bucket, Key):
        assert Bucket == "some-bucket"
        assert Key == self._key
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


def test_s3_xgboost_booster_matches_local_predictions(tmp_path):
    import pandas as pd  # pylint: disable=import-outside-toplevel

    rng = np.random.default_rng(0)
    frame = pd.DataFrame(
        {
            "a": rng.random(80),
            "b": rng.random(80),
            "map_name": pd.Categorical(rng.choice(["de_dust2", "de_mirage"], 80)),
        }
    )
    target = frame["a"].to_numpy() + rng.random(80)
    dtrain = xgboost.DMatrix(frame, label=target, enable_categorical=True)
    trained = xgboost.train({"max_depth": 2, "seed": 0}, dtrain, num_boost_round=4)
    artifact = tmp_path / "model_v1.json"
    trained.save_model(artifact)

    models = create_ds_models(models=MODELS, log=get_logger())
    ds_model = models.get_ds_model("booster_test")
    ds_model._s3_client = FakeS3Client(
        artifact.read_bytes(), key="department/test/booster_test/model_v1.json"
    )

    predictions = ds_model.invoke(frame)
    expected = trained.predict(xgboost.DMatrix(frame, enable_categorical=True))
    np.testing.assert_allclose(predictions, expected, rtol=1e-6)
