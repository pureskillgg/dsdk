from io import BytesIO
from gzip import GzipFile
from typing import List, Dict, Optional, Union

import structlog
import rapidjson
import boto3
import pandas as pd
from .handle_value_error import handle_value_error


class DsReaderS3:
    def __init__(
        self,
        *,
        bucket: str,
        log: object = None,
        manifest_key: Optional[str] = None,
        prefix: Optional[str] = None,
    ):
        self._log = log if log is not None else structlog.get_logger()
        self._log = self._log.bind(client="ds_reader_s3", bucket=bucket, prefix=prefix)
        self._bucket = bucket
        self._prefix = prefix
        self._s3_client = boto3.client("s3")
        self._manifest_key = manifest_key

    def read_manifest(self) -> Dict:
        self._log.info("Read Manifest: Start")

        if self._manifest_key is None:
            raise Exception("No manifest key given")

        key = add_prefix(self._manifest_key, self._prefix)
        return self._get_json_object(key)

    def read_metadata(self) -> Dict:
        self._log.info("Read Metadata: Start")

        if self._manifest_key is None:
            raise Exception("No manifest key given")

        key = add_prefix(self._manifest_key, self._prefix)
        return self._get_metadata(key)

    def read_parquet_channel(self, channel: Dict, columns: List[str]) -> pd.DataFrame:
        self._log.info(
            "Read parquet Channel: Start", channel=channel["channel"], columns=columns
        )

        try:
            # UPSTREAM: Flaky, will sometimes throw FileNotFoundError on s3 objects
            # https://github.com/apache/arrow/issues/2192#issuecomment-569813829
            try:
                df = pd.read_parquet(
                    self._get_channel_location(channel), columns=columns
                )
            except FileNotFoundError:
                key = self._get_channel_key(channel)
                obj = self._s3_client.get_object(Bucket=self._bucket, Key=key)
                df = pd.read_parquet(BytesIO(obj["Body"].read()), columns=columns)
        except ValueError as value_error:
            df = handle_value_error(value_error, channel)

        return df

    def _get_json_object(self, key: str, /) -> Dict:
        res = self._s3_client.get_object(Bucket=self._bucket, Key=key)

        content_encoding = res["ContentEncoding"]

        body = res["Body"].read()
        if content_encoding == "gzip":
            body = BytesIO(body)
            body = GzipFile(fileobj=body, mode="rb").read().decode("utf-8")
        else:
            body = body.decode("utf-8")

        return rapidjson.loads(body)

    def _get_metadata(self, key: str, /) -> Dict:
        res = self._s3_client.head_object(Bucket=self._bucket, Key=key)
        return {
            **res["Metadata"],
            "key": key,
            "bucket": self._bucket,
            "content_type": res["ContentType"],
            "last_modified": res["LastModified"].isoformat(),
        }

    def _get_compression(self, channel: Dict, /) -> Union[str, None]:
        key = self._get_channel_key(channel)
        res = self._s3_client.head_object(Bucket=self._bucket, Key=key)

        content_encoding = res["ContentEncoding"]

        if content_encoding == "gzip":
            return "gzip"

        return None

    def _get_channel_location(self, channel: Dict, /) -> str:
        key = self._get_channel_key(channel)
        return f"s3://{self._bucket}/{key}"

    def _get_channel_key(self, channel: Dict) -> str:
        key = channel["key"]
        prefix = self._prefix
        return add_prefix(key, prefix)


def add_prefix(key, prefix: Union[str, None], /) -> str:
    if prefix is None:
        return key
    return "/".join([prefix, key])
