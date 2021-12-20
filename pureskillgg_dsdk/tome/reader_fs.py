import pandas as pd
import structlog
import rapidjson
from .constants import (
    get_page_key_fs,
    get_tome_manifest_key_fs,
    get_tome_path_fs,
)


class TomeReaderFs:
    def __init__(self, *, root_path, prefix=None, tome_name, log=None, has_header=True):
        self._log = log if log is not None else structlog.get_logger()
        self._log = self._log.bind(
            client="tome_reader_fs",
            root_path=root_path,
            prefix=prefix,
            tome_name=tome_name,
        )

        self._path = get_tome_path_fs(root_path, prefix, tome_name)
        self.has_header = has_header
        self.header = None
        if self.has_header:
            self.header = TomeReaderFs(
                root_path=self._path,
                tome_name="header",
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
        key = get_tome_manifest_key_fs(self._path)
        with open(key, "r", encoding="utf-8") as file:
            data = rapidjson.loads(file.read())
        return data

    # pylint: disable=no-self-use
    def read_metadata(self):
        return {}

    def read_page(self, page):
        dataframe = self.read_page_dataframe(page)
        keyset = self.read_page_keyset(page)
        return dataframe, keyset

    def read_page_keyset(self, page):
        key = self._get_page_key("keyset", page)

        content_type = page["keysetContentType"]
        if content_type != "application/x-parquet":
            raise Exception(f"Unknown content type {content_type}")

        self._log.info("Read keyset: Start", page_number=page["number"])
        df = pd.read_parquet(key)
        return list(df.iloc[:, 0])

    def read_page_dataframe(self, page):
        key = self._get_page_key("dataframe", page)

        content_type = page["dataframeContentType"]
        if content_type != "application/x-parquet":
            raise Exception(f"Unsupported content type {content_type}")

        self._log.info("Read Dataframe: Start", page_number=page["number"])
        df = pd.read_parquet(key)
        return df

    def _get_page_key(self, subtype, page):
        return get_page_key_fs(self._path, subtype, page)
