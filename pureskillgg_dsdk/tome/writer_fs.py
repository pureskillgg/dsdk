import os

from pathlib import Path
import structlog
import rapidjson
import pandas as pd

from .constants import get_page_path_fs


class TomeWriterFs:
    def __init__(
        self,
        *,
        root_path,
        prefix=None,
        log=None,
    ):
        self._log = log if log is not None else structlog.get_logger()
        self._log = self._log.bind(
            client="tome_writer_fs",
            root_path=root_path,
            prefix=prefix,
        )
        self._root_path = root_path
        self._prefix = prefix
        self._parquet_compression = "gzip"

    def write_manifest(self, manifest):
        file_location = os.path.join(
            self._root_path, add_prefix(manifest["key"], self._prefix)
        )
        ensure_dir(file_location)
        self._log.info("Write Manifest: Start")

        self._write_json(file_location, manifest)

    def write_page(self, page, dataframe, keyset):
        ensure_dir(self._get_page_key("dataframe", page))
        self._log.info("Write Page Start", page_number=page["number"])
        self._write_dataframe(page, dataframe)
        self._write_keyset(page, keyset)

    def _write_dataframe(self, page, dataframe):
        key = self._get_page_key("dataframe", page)

        content_type = page["dataframe"]["contentType"]
        if content_type != "application/x-parquet":
            raise Exception(f"Unsupported content type {content_type}")

        self._log.info("Write Dataframe: Start", page_number=page["number"])
        self._write_parquet(key, dataframe)

    def _write_keyset(self, page, keyset):
        key = self._get_page_key("keyset", page)

        content_type = page["keyset"]["contentType"]
        if content_type != "application/x-parquet":
            raise Exception(f"Unsupported content type {content_type}")

        self._log.info("Write keyset: Start", page_number=page["number"])
        df = pd.DataFrame(
            keyset, columns=["_"]
        )  # must have string column name for parquet
        self._write_parquet(key, df)

    def _get_page_key(self, subtype, page):
        return get_page_path_fs(self._root_path, subtype, page)

    def _write_parquet(self, key: str, df: pd.DataFrame) -> None:
        self._log.debug("Write parquet", key=key)
        df.to_parquet(key, compression=self._parquet_compression)

    def _write_json(self, key, data):
        with open(key, "w", encoding="utf-8") as file:
            rapidjson.dump(data, file)


def add_prefix(key, prefix, /) -> str:
    if prefix is None:
        return key
    return os.path.join(*[*prefix.split("/"), key])


def ensure_dir(file_location):
    folder = Path(file_location).parent
    if not os.path.isdir(folder):
        os.makedirs(folder)
