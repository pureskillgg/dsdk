# pylint: disable=missing-docstring,invalid-name,protected-access

import gzip
import io
import json
import os
from datetime import datetime, timezone

import pandas as pd
import pytest

from .reader_s3 import DsReaderS3

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

BUCKET = "some-bucket"
MANIFEST_KEY = "csds/2022/01/01/test-match/csds"
MANIFEST = {"id": "test-match", "key": MANIFEST_KEY, "channels": []}
CHANNEL = {
    "channel": "round_end",
    "key": "csds/2022/01/01/test-match/round_end",
    "contentType": "application/x-parquet",
    "columns": [{"name": "tick"}, {"name": "winner"}],
}
FRAME = pd.DataFrame({"tick": [100, 200], "winner": ["t", "ct"]})


class StubS3Client:
    """Answers from memory; nothing here reaches S3."""

    def __init__(self, objects=None, heads=None):
        self._objects = objects or {}
        self._heads = heads or {}
        self.calls = []

    def get_object(self, Bucket, Key):
        self.calls.append(("get_object", Bucket, Key))
        res = dict(self._objects[Key])
        res["Body"] = io.BytesIO(res["Body"])
        return res

    def head_object(self, Bucket, Key):
        self.calls.append(("head_object", Bucket, Key))
        return self._heads[Key]


def create_reader(client, *, prefix=None, manifest_key=MANIFEST_KEY):
    reader = DsReaderS3(bucket=BUCKET, manifest_key=manifest_key, prefix=prefix)
    reader._s3_client = client
    return reader


def intercept_s3_reads(monkeypatch, on_s3_read):
    """Route pandas reads of s3:// paths to on_s3_read instead of s3fs."""
    attempts = []
    real_read_parquet = pd.read_parquet

    def read_parquet(path, *args, **kwargs):
        if isinstance(path, str) and path.startswith("s3://"):
            attempts.append(path)
            return on_s3_read(path, *args, **kwargs)
        return real_read_parquet(path, *args, **kwargs)

    monkeypatch.setattr(pd, "read_parquet", read_parquet)
    return attempts


def raising(error):
    def on_s3_read(*_args, **_kwargs):
        raise error

    return on_s3_read


def to_parquet_bytes(frame):
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def test_read_manifest_gzip():
    body = gzip.compress(json.dumps(MANIFEST).encode("utf-8"))
    client = StubS3Client({MANIFEST_KEY: {"Body": body, "ContentEncoding": "gzip"}})
    reader = create_reader(client)

    assert reader.read_manifest() == MANIFEST
    assert client.calls == [("get_object", BUCKET, MANIFEST_KEY)]


def test_read_manifest_without_content_encoding():
    body = json.dumps(MANIFEST).encode("utf-8")
    client = StubS3Client({MANIFEST_KEY: {"Body": body}})
    reader = create_reader(client)

    assert reader.read_manifest() == MANIFEST


def test_read_manifest_with_prefix():
    key = f"some/prefix/{MANIFEST_KEY}"
    client = StubS3Client({key: {"Body": json.dumps(MANIFEST).encode("utf-8")}})
    reader = create_reader(client, prefix="some/prefix")

    assert reader.read_manifest() == MANIFEST
    assert client.calls == [("get_object", BUCKET, key)]


def test_read_without_manifest_key():
    reader = create_reader(StubS3Client(), manifest_key=None)

    with pytest.raises(Exception, match="No manifest key given"):
        reader.read_manifest()
    with pytest.raises(Exception, match="No manifest key given"):
        reader.read_metadata()


def test_read_metadata():
    key = f"some/prefix/{MANIFEST_KEY}"
    last_modified = datetime(2022, 1, 1, 12, 30, tzinfo=timezone.utc)
    client = StubS3Client(
        heads={
            key: {
                "Metadata": {"source": "test"},
                "ContentType": "application/json",
                "LastModified": last_modified,
            }
        }
    )
    reader = create_reader(client, prefix="some/prefix")

    assert reader.read_metadata() == {
        "source": "test",
        "key": key,
        "bucket": BUCKET,
        "content_type": "application/json",
        "last_modified": "2022-01-01T12:30:00+00:00",
    }
    assert client.calls == [("head_object", BUCKET, key)]


def test_read_parquet_channel_from_s3_location(monkeypatch):
    client = StubS3Client()
    reader = create_reader(client, prefix="some/prefix")
    attempts = intercept_s3_reads(monkeypatch, lambda *_args, **_kwargs: FRAME)

    df = reader.read_parquet_channel(CHANNEL, columns=None)

    pd.testing.assert_frame_equal(df, FRAME)
    assert attempts == [f"s3://{BUCKET}/some/prefix/{CHANNEL['key']}"]
    assert not client.calls


def test_read_parquet_channel_falls_back_to_get_object(monkeypatch):
    key = f"some/prefix/{CHANNEL['key']}"
    client = StubS3Client({key: {"Body": to_parquet_bytes(FRAME)}})
    reader = create_reader(client, prefix="some/prefix")
    attempts = intercept_s3_reads(monkeypatch, raising(FileNotFoundError(key)))

    df = reader.read_parquet_channel(CHANNEL, columns=["tick"])

    pd.testing.assert_frame_equal(df, FRAME[["tick"]])
    assert attempts == [f"s3://{BUCKET}/{key}"]
    assert client.calls == [("get_object", BUCKET, key)]


def test_read_parquet_channel_empty(monkeypatch):
    reader = create_reader(StubS3Client())
    error = ValueError("need at least one array to concatenate")
    intercept_s3_reads(monkeypatch, raising(error))

    df = reader.read_parquet_channel(CHANNEL, columns=None)

    assert df.empty
    assert list(df.columns) == ["tick", "winner"]


def test_read_parquet_channel_error_keeps_cause(monkeypatch):
    reader = create_reader(StubS3Client())
    error = ValueError("corrupt file")
    intercept_s3_reads(monkeypatch, raising(error))

    with pytest.raises(Exception, match="round_end") as excinfo:
        reader.read_parquet_channel(CHANNEL, columns=None)
    assert excinfo.value.__cause__ is error
