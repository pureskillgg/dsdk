# pylint: disable=missing-docstring,invalid-name
# pylint: disable=no-value-for-parameter

import io
import os

import pandas as pd
import pytest
from structlog import get_logger

from .model import create_ds_models

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")


def create_model_config(res_type="application/x-parquet"):
    return {
        "frames_test": [
            {
                "type": "s3_dataframe_set",
                "model_name": "frames_test_v1",
                "bucket": "some-bucket",
                "prefix": "department/test/frames_test",
                "extension": "parquet",
                "res_type": res_type,
                "dataframes": [
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


def create_ds_model(res_type, body=b""):
    models = create_ds_models(models=create_model_config(res_type), log=get_logger())
    ds_model = models.get_ds_model("frames_test")
    client = CountingS3Client(body)
    ds_model._s3_client = client  # pylint: disable=protected-access
    return ds_model, client


def test_s3_dataframe_set_downloads_once(tmp_path):
    frame = pd.DataFrame({"map_name": ["de_dust2"], "value": [0.5]})
    artifact = tmp_path / "dust2_v1.parquet"
    frame.to_parquet(artifact, index=False)
    ds_model, client = create_ds_model("application/x-parquet", artifact.read_bytes())
    ds_model.select({"map_name": "de_dust2"})

    pd.testing.assert_frame_equal(ds_model.invoke(), frame)
    pd.testing.assert_frame_equal(ds_model.invoke(), frame)
    assert client.keys == ["department/test/frames_test/dust2_v1.parquet"]


def test_s3_dataframe_set_unknown_res_type():
    ds_model, _ = create_ds_model("text/csv")
    ds_model.select({"map_name": "de_dust2"})

    with pytest.raises(Exception, match="Unknown res_type text/csv"):
        ds_model.invoke()
