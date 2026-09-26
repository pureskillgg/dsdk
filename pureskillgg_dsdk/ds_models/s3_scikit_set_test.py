# pylint: disable=missing-docstring,invalid-name

import io
import pickle

import pandas as pd
from structlog import get_logger

from .model import create_ds_models

MODELS = {
    "clusters_test": [
        {
            "type": "s3_scikit_set",
            "model_name": "clusters_test_v1",
            "model_type": "hdbscan",
            "bucket": "some-bucket",
            "prefix": "department/test/clusters_test",
            "extension": "pkl",
            "res_type": "application/x-pickle",
            "scikits": [
                {"key": "dust2_v1", "map_name": "de_dust2"},
                {"key": "mirage_v1", "map_name": "de_mirage"},
            ],
        }
    ]
}


class CountingS3Client:
    def __init__(self, body):
        self._body = body
        self.keys = []

    def get_object(self, Bucket, Key):
        assert Bucket == "some-bucket"
        self.keys.append(Key)
        return {"Body": io.BytesIO(self._body)}


class StubHdbscan:
    def __init__(self):
        self.models = []

    def approximate_predict(self, model, data):
        self.models.append(model)
        return [0] * len(data), [1.0] * len(data)


def test_s3_scikit_set_downloads_once():
    hdbscan = StubHdbscan()
    models = create_ds_models(hdbscan=hdbscan, models=MODELS, log=get_logger())
    ds_model = models.get_ds_model("clusters_test")
    client = CountingS3Client(pickle.dumps({"clusters": 3}))
    ds_model._s3_client = client  # pylint: disable=protected-access
    ds_model.select({"map_name": "de_dust2"})
    frame = pd.DataFrame({"x": [0.1, 0.2]})

    assert ds_model.invoke(frame) == [0, 0]
    assert ds_model.invoke(frame) == [0, 0]
    assert client.keys == ["department/test/clusters_test/dust2_v1.pkl"]
    assert hdbscan.models[0] == {"clusters": 3}
    assert hdbscan.models[1] is hdbscan.models[0]
