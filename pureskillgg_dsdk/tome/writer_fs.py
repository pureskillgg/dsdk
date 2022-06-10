import os

from pathlib import Path
import structlog
import rapidjson
import pandas as pd

from .constants import (
    get_tome_manifest_key_fs,
    get_page_key_fs,
)


class TomeWriterFs:
    def __init__(
        self,
        *,
        root_path,
        prefix=None,
        tome_name,
        ds_type,
        is_copied_header=False,
        log=None,
    ):
        self._log = log if log is not None else structlog.get_logger()
        self._log = self._log.bind(
            client="tome_writer_fs",
            root_path=root_path,
            prefix=prefix,
            tome_name=tome_name,
        )
        self.tome_name = tome_name
        self.path = root_path if prefix is None else os.path.join(root_path, prefix)
        self.ds_type = ds_type
        self._parquet_compression = "gzip"
        self._is_copied_header = is_copied_header

    def write_manifest(self, manifest):
        self._ensure_dir()
        self._log.info("Write Manifest: Start")
        key = get_tome_manifest_key_fs(
            self.path, self.ds_type, self.tome_name, self._is_copied_header
        )
        self._write_json(key, manifest)

    def write_page(self, page, dataframe, keyset):
        self._ensure_dir()
        self._log.info("Write Page Start", page_number=page["number"])
        self._write_dataframe(page, dataframe)
        self._write_keyset(page, keyset)

    def _ensure_dir(self):
        key = get_tome_manifest_key_fs(
            self.path, self.ds_type, self.tome_name, self._is_copied_header
        )
        folder = Path(key).parent
        if not os.path.isdir(folder):
            os.makedirs(folder)

    def _write_dataframe(self, page, dataframe):
        key = self._get_page_key("dataframe", page)

        content_type = page["dataframe"]["ContentType"]
        if content_type != "application/x-parquet":
            raise Exception(f"Unsupported content type {content_type}")

        self._log.info("Write Dataframe: Start", page_number=page["number"])
        self._write_parquet(key, dataframe)

    def _write_keyset(self, page, keyset):
        key = self._get_page_key("keyset", page)

        content_type = page["keyset"]["ContentType"]
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
