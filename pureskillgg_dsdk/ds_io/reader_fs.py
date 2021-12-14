import os.path
from gzip import GzipFile
from typing import List, Dict, Optional, Union

import structlog
import rapidjson
import pandas as pd
from .handle_value_error import handle_value_error


class DsReaderFs:
    def __init__(
        self,
        *,
        root_path: str,
        log: object = None,
        manifest_key: Optional[str] = None,
        prefix: Optional[str] = None,
    ):
        self._log = log if log is not None else structlog.get_logger()
        self._log = self._log.bind(
            client="ds_reader_fs", root_path=root_path, manifest_key=manifest_key
        )
        self._is_gzipped = True
        self._root_path = root_path
        self._manifest_key = manifest_key
        self._prefix = prefix

    def read_manifest(self) -> Dict:
        self._log.info("Read Manifest: Start")

        file_location = os.path.join(
            self._root_path, add_prefix(self._manifest_key, self._prefix)
        )
        if self._is_gzipped:
            with GzipFile(file_location, "r") as fin:
                data = rapidjson.loads(fin.read().decode("utf-8"))
        else:
            with open(file_location, "r", encoding="utf-8") as fin:
                data = rapidjson.loads(fin.read())

        return data

    def read_metadata(self) -> Dict:
        self._log.info("Read Metadata: Start")
        return {}

    def read_parquet_channel(self, channel: Dict, columns: List[str]) -> pd.DataFrame:
        self._log.info(
            "Read parquet Channel: Start", channel=channel["channel"], columns=columns
        )
        file_location = self._get_file_location(channel)
        try:
            df = pd.read_parquet(file_location, columns=columns)
        except ValueError as value_error:
            df = handle_value_error(value_error, channel)

        return df

    def _get_file_location(self, channel, /) -> str:
        key = self._get_file_path(channel)
        return os.path.join(self._root_path, key)

    def _get_file_path(self, channel: Union[str, Dict]) -> str:
        if isinstance(channel, str):
            key = os.path.join(
                os.path.normpath(self._manifest_key).split(os.path.sep)[:-1]
            )

            key = os.path.join(key, channel)

        elif isinstance(channel, dict):
            key = os.path.normpath(channel["key"])
        else:
            raise Exception(f"Unknown channel type {type(channel)}")
        return add_prefix(key, self._prefix)


def add_prefix(key, prefix, /) -> str:
    if prefix is None:
        return key
    return os.path.join(prefix, key)
