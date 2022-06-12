import os
from pathlib import Path
import pandas as pd
import structlog
import rapidjson
from .constants import (
    get_page_path_fs,
)


class TomeReaderFs:
    def __init__(
        self,
        *,
        root_path,
        prefix=None,
        manifest_key,
        has_header=True,
        log=None,
    ):
        self._log = log if log is not None else structlog.get_logger()
        self._log = self._log.bind(
            client="tome_reader_fs",
            root_path=root_path,
            prefix=prefix,
            manifest_key=manifest_key,
        )

        self._root_path = root_path
        self._prefix = prefix
        self._manifest_key = manifest_key
        self.has_header = has_header
        self.header = None
        if self.has_header:
            self.header = TomeReaderFs(
                root_path=self._root_path,
                manifest_key="/".join(
                    [str(Path(self._manifest_key).parent), "header", "tome"]
                ),
                has_header=False,
                log=self._log,
            )

    @property
    def exists(self):
        """If the tome exists"""
        try:
            self.read_manifest()
        except FileNotFoundError:
            return False
        except:
            self._log.error("There was an error while loading the loader")
            raise
        return True

    def read_manifest(self):
        self._log.info("Read Manifest: Start")
        file_location = os.path.join(
            self._root_path, add_prefix(self._manifest_key, self._prefix)
        )

        with open(file_location, "r", encoding="utf-8") as file:
            data = rapidjson.loads(file.read())
        return data

    def read_metadata(self):
        return {}

    def read_page(self, page):
        dataframe = self.read_page_dataframe(page)
        keyset = self.read_page_keyset(page)
        return dataframe, keyset

    def read_page_keyset(self, page):
        key = self._get_page_key("keyset", page)

        content_type = page["keyset"]["contentType"]
        if content_type != "application/x-parquet":
            raise Exception(f"Unknown content type {content_type}")

        self._log.info("Read keyset: Start", page_number=page["number"])
        df = pd.read_parquet(key)
        return list(df.iloc[:, 0])

    def read_page_dataframe(self, page):
        key = self._get_page_key("dataframe", page)

        content_type = page["dataframe"]["contentType"]
        if content_type != "application/x-parquet":
            raise Exception(f"Unsupported content type {content_type}")

        self._log.info("Read Dataframe: Start", page_number=page["number"])
        df = pd.read_parquet(key)
        return df

    def _get_page_key(self, subtype, page):
        return get_page_path_fs(self._root_path, subtype, page)


def add_prefix(key, prefix, /) -> str:
    if prefix is None:
        return key
    return os.path.join(*[*prefix.split("/"), key])
