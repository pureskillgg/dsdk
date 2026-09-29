from typing import TYPE_CHECKING, Literal, Optional, Sequence, overload

import structlog
import pandas as pd

from .page_frames import read_pages_pandas, scan_pages_polars

if TYPE_CHECKING:
    import polars as pl

LIBRARIES = ("pandas", "polars")


class TomeLoader:
    def __init__(self, *, reader, has_header=True, log: object = None):
        self._reader = reader
        self._log = log if log is not None else structlog.get_logger()
        self._manifest = None
        self._metadata = None
        self._exists = None
        self.has_header = has_header
        self.header = None

        if self.has_header:
            self.header = TomeLoader(
                reader=self._reader.header, has_header=False, log=self._log
            )

    @property
    def metadata(self):
        """Metadata"""
        self._load()
        return self._metadata

    @property
    def exists(self):
        """If the tome exists"""
        return self._reader.exists

    @property
    def is_complete(self):
        """If the tome is complete"""
        if not self._reader.exists:
            return False
        return self.manifest.get("isComplete", False)

    @property
    def manifest(self):
        """Tome manifest"""
        self._load()
        return self._manifest

    def _load(self) -> None:
        is_loaded = self._manifest is not None and self._metadata is not None
        if is_loaded:
            return
        self._metadata = self._reader.read_metadata()
        self._manifest = self._reader.read_manifest()

    @overload
    def get_dataframe(
        self,
        *,
        columns: Optional[Sequence[str]] = None,
        library: Literal["pandas"] = "pandas",
    ) -> pd.DataFrame: ...

    @overload
    def get_dataframe(
        self,
        *,
        columns: Optional[Sequence[str]] = None,
        library: Literal["polars"],
    ) -> "pl.DataFrame": ...

    def get_dataframe(self, *, columns=None, library="pandas"):
        """
        Read every page of the tome into one frame.

        Parameters
        ----------
        columns : list of str, default=None
            Columns to read, in this order. None reads every column. A
            column some pages lack is null on their rows.
        library : {"pandas", "polars"}, default="pandas"
            "polars" returns a polars DataFrame, and needs the ``polars``
            extra (``pureskillgg-dsdk[polars]``).

        Returns
        -------
        pd.DataFrame or pl.DataFrame
            The pages' rows in page order. A pandas frame has one
            ``RangeIndex`` (0 to n - 1), and each column has the dtype
            ``pd.concat`` of the pages gives it.
        """
        if library not in LIBRARIES:
            raise ValueError(f"library must be one of {LIBRARIES}, not {library!r}")
        paths = self._get_page_dataframe_paths()
        self._log.info("Read Dataframe: Start", pages=len(paths), library=library)
        if library == "polars":
            return scan_pages_polars(paths, columns).collect()
        return read_pages_pandas(paths, columns)

    def scan(self) -> "pl.LazyFrame":
        """
        Scan every page of the tome as one polars LazyFrame, so a query reads
        only the columns and rows it needs. Needs the ``polars`` extra
        (``pureskillgg-dsdk[polars]``).

        Returns
        -------
        pl.LazyFrame
            The pages' rows in page order. Old pages' pandas index columns
            are dropped, and pages whose columns differ are joined with
            ``pl.concat(how="diagonal_relaxed")``.
        """
        return scan_pages_polars(self._get_page_dataframe_paths())

    def _get_page_dataframe_paths(self):
        self._load()
        pages = self.manifest["pages"]
        if len(pages) == 0:
            raise ValueError("The tome has no pages")
        return [self._reader.get_page_dataframe_path(page) for page in pages]

    def get_keyset(self):
        self._load()

        keyset = []
        for page in self.manifest["pages"]:
            keyset += self._reader.read_page_keyset(page)
        return keyset

    def iterate_pages(self):
        self._load()
        for page in self.manifest["pages"]:
            yield (
                self._reader.read_page_dataframe(page),
                self._reader.read_page_keyset(page),
            )
