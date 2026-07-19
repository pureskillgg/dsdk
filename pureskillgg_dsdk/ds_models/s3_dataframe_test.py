# pylint: disable=missing-docstring,invalid-name,protected-access

import io
import os

import pandas as pd
from structlog import get_logger

from .model import create_ds_models

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

MODELS = {
    "frame_test": [
        {
            "type": "s3_dataframe",
            "model_name": "frame_test_v1",
            "bucket": "some-bucket",
            "key": "department/test/frame_test/frame_v1.parquet",
            "res_type": "application/x-parquet",
        }
    ]
}


class FakeS3Client:
    def __init__(self, body):
        self._body = body

    def get_object(self, Bucket, Key):
        assert Bucket == "some-bucket"
        assert Key == "department/test/frame_test/frame_v1.parquet"
        return {"Body": io.BytesIO(self._body)}


def test_s3_dataframe_parquet_round_trip(tmp_path):
    frame = pd.DataFrame({"map_name": ["de_dust2", "de_mirage"], "value": [0.25, 0.75]})
    artifact = tmp_path / "frame_v1.parquet"
    frame.to_parquet(artifact, index=False)

    models = create_ds_models(models=MODELS, log=get_logger())
    ds_model = models.get_ds_model("frame_test")
    ds_model._s3_client = FakeS3Client(artifact.read_bytes())

    loaded = ds_model.invoke()
    pd.testing.assert_frame_equal(loaded, frame)
