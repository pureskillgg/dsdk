import io
import os

from pathlib import Path
import structlog
import rapidjson
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .constants import DEFAULT_PAGE_COMPRESSION, get_page_path_fs


class TomeWriterFs:
    """
    Write tome pages and manifests to disk.

    Parameters
    ----------
    root_path : str
        Folder the tome keys are written under.
    prefix : str, default=None
        Optional prefix for the manifest key.
    compression : str or None, default="zstd"
        Parquet codec for the page files, passed to ``DataFrame.to_parquet``:
        any codec it accepts, such as ``"gzip"`` for the format dsdk 3.2.2
        and earlier wrote, or ``None`` for uncompressed pages. Each page is
        its own parquet file, so pages of one tome may use different codecs.
        A codec parquet can't write raises ``ValueError`` here.
    log : structlog.stdlib.BoundLogger, default=None
        Logger.
    """

    def __init__(
        self,
        *,
        root_path,
        prefix=None,
        compression=DEFAULT_PAGE_COMPRESSION,
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
        self._parquet_compression = compression
        check_compression(compression)

    def write_manifest(self, manifest):
        file_location = os.path.join(
            self._root_path, add_prefix(manifest["key"], self._prefix)
        )
        ensure_dir(file_location)
        self._log.info("Write Manifest: Start")

        self._write_json(file_location, manifest)

    def write_page(self, page, dataframe, keyset):
        """
        Write one page: its rows and its keyset.

        dataframe is a pandas ``DataFrame``, or a ``pyarrow.Table`` that is
        written as it is, pandas metadata included.
        """
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

    def _write_parquet(self, key: str, df: pd.DataFrame | pa.Table) -> None:
        self._log.debug("Write parquet", key=key)
        if isinstance(df, pa.Table):
            pq.write_table(df, key, compression=self._parquet_compression)
            return
        df.to_parquet(key, compression=self._parquet_compression)

    def _write_json(self, key, data):
        with open(key, "w", encoding="utf-8") as file:
            rapidjson.dump(data, file)


def check_compression(compression, /) -> None:
    """Fail now on a codec parquet can't write, not when the first page is full."""
    try:
        pd.DataFrame({"_": [0]}).to_parquet(io.BytesIO(), compression=compression)
    except Exception as err:
        raise ValueError(
            f"Unsupported parquet compression for tome pages: {compression!r}"
        ) from err


def add_prefix(key, prefix, /) -> str:
    if prefix is None:
        return key
    return os.path.join(*[*prefix.split("/"), key])


def ensure_dir(file_location):
    folder = Path(file_location).parent
    if not os.path.isdir(folder):
        os.makedirs(folder)
