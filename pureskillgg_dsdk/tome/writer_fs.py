import os

import structlog
import rapidjson
import pandas as pd

from .constants import (
    get_tome_manifest_key_fs,
    get_page_key_fs,
    get_tome_path_fs,
)


class TomeWriterFs:
    def __init__(self, *, root_path, prefix=None, tome_name, log=None):
        self._log = log if log is not None else structlog.get_logger()
        self._log = self._log.bind(
            client="tome_writer_fs",
            root_path=root_path,
            prefix=prefix,
            tome_name=tome_name,
        )
        self.tome_name = tome_name
        self.path = get_tome_path_fs(root_path, prefix, tome_name)
        self._parquet_compression = "gzip"

    def write_manifest(self, manifest):
        self._ensure_dir()
        self._log.info("Write Manifest: Start")
        key = get_tome_manifest_key_fs(self.path)
        self._write_json(key, manifest)

    def write_page(self, page, dataframe, keyset):
        self._ensure_dir()
        self._log.info("Write Page Start", page_number=page["number"])
        self._write_dataframe(page, dataframe)
        self._write_keyset(page, keyset)

    def _ensure_dir(self):
        if not os.path.isdir(self.path):
            os.makedirs(self.path)

    def _write_dataframe(self, page, dataframe):
        key = self._get_page_key("dataframe", page)

        content_type = page["dataframeContentType"]
        if content_type != "application/x-parquet":
            raise Exception(f"Unsupported content type {content_type}")

        self._log.info("Write Dataframe: Start", page_number=page["number"])
        self._write_parquet(key, dataframe)

    def _write_keyset(self, page, keyset):
        key = self._get_page_key("keyset", page)

        content_type = page["keysetContentType"]
        if content_type != "application/x-parquet":
            raise Exception(f"Unsupported content type {content_type}")

        self._log.info("Write keyset: Start", page_number=page["number"])
        df = pd.DataFrame(
            keyset, columns=["_"]
        )  # must have string column name for parquet
        self._write_parquet(key, df)

    def _get_page_key(self, subtype, page):
        return get_page_key_fs(self.path, subtype, page)

    def _write_parquet(self, key: str, df: pd.DataFrame) -> None:
        self._log.debug("Write parquet", key=key)
        df.to_parquet(key, compression=self._parquet_compression)

    # pylint: disable=no-self-use
    def _write_json(self, key, data):
        with open(key, "w", encoding="utf-8") as file:
            rapidjson.dump(data, file)
