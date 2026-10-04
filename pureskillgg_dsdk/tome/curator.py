import os
import random
import warnings
from typing import TYPE_CHECKING, List, Literal, Optional, Sequence, overload
import structlog
import pandas as pd

from .header_tome import create_header_tome_from_fs, create_subheader_tome_from_fs
from .builder import (
    DEFAULT_MAX_PAGE_SIZE_MB,
    DEFAULT_READ_THREADS,
    DEFAULT_TOME_NAME,
    BasicTomes,
    build_basic_tomes_from_fs,
)

from .loader import TomeLoader
from .scribe import TomeScribe
from .manifest import TomeManifest
from .maker import TomeMaker
from .writer_fs import TomeWriterFs
from .reader_fs import TomeReaderFs
from .header_copier_fs import HeaderTomeCopierFs
from .constants import DEFAULT_PAGE_COMPRESSION, warn_if_invalid_tome_name

from ..ds_io import ChannelInstruction, DsReaderFs, GameDsLoader

if TYPE_CHECKING:
    import polars as pl


class TomeCuratorFs:
    """
    Simple API to manage tomes.

    Parameters
    ----------
    default_header_name : str, default=from env (PURESKILLGG_TOME_DEFAULT_HEADER_NAME)
        Name of the default header.
    ds_type : str, default=from env (PURESKILLGG_TOME_DS_TYPE)
        Type of data science file to read from `ds_collection_root_path`.
    tome_collection_root_path : str, default=from env (PURESKILLGG_TOME_COLLECTION_PATH)
        Path leading to where tomes will be stored.
    ds_collection_root_path : str, default=from env (PURESKILLGG_TOME_DS_COLLECTION_PATH)
        Path leading to a series of (possibly nested) folders containing game Data Science files.
    log : structlog.stdlib.BoundLogger
        Logger used for logging logs.

    See Also
    --------
    TomeMaker : Make tomes.
    """

    def __init__(
        self,
        *,
        default_header_name: str = None,
        ds_type: str = None,
        tome_collection_root_path: str = None,
        ds_collection_root_path: str = None,
        log: structlog.stdlib.BoundLogger = None,
    ):
        self._log = log if log is not None else structlog.get_logger()
        self._default_header_name = get_env_option(
            "default_header_name", default_header_name
        )
        self._ds_type = get_env_option("ds_type", ds_type)
        self._tome_collection_root_path = get_env_option(
            "collection_path", tome_collection_root_path
        )
        self._ds_collection_root_path = get_env_option(
            "ds_collection_path", ds_collection_root_path
        )

    def create_header_tome(
        self,
        tome_name: str = None,
        /,
        *,
        path_depth=None,
        compression: str | None = DEFAULT_PAGE_COMPRESSION,
    ) -> TomeLoader:
        """
        Create the header tome.

        Parameters
        ----------
        tome_name : str, default=`default_header_name`
            Name of the header that will be created.
        path depth : int, default=4
            DEPRECATED. Please do not use. Search is recursive.
        compression : str or None, default="zstd"
            Parquet codec for the tome's pages (see `make_tome`).

        Returns
        -------
        TomeLoader
            Loader for the header tome just created.
        """
        if path_depth is not None:
            warnings.warn(
                "Warning: the keyword path_depth is deprecated. It is not needed."
            )
        name = tome_name if tome_name is not None else self._default_header_name
        warn_if_invalid_tome_name(name)
        return create_header_tome_from_fs(
            name,
            ds_type=self._ds_type,
            tome_collection_root_path=self._tome_collection_root_path,
            ds_collection_root_path=self._ds_collection_root_path,
            compression=compression,
            log=self._log,
        )

    def create_subheader_tome(
        self,
        tome_name: str,
        selector: callable = lambda df: [True] * len(df),
        /,
        *,
        src_tome_name: str = None,
        compression: str | None = DEFAULT_PAGE_COMPRESSION,
    ) -> TomeLoader:
        """
        Create a subheader tome.

        Parameters
        ----------
        tome_name : str, default=`default_header_name`
            Name of the header that will be created.
        selector : callable, default=lambda to select all rows
            The selector is passed directly through to the header
            dataframe and the final subheader tome will be equal to
            `header_dataframe.loc[selector]`.
        src_tome_name : str, default=`default_header_name`
            Source header file. Should be the same as the
            default header name in most cases.
        compression : str or None, default="zstd"
            Parquet codec for the tome's pages (see `make_tome`).

        Returns
        -------
        TomeLoader
            Loader for the header tome just created.
        """
        warn_if_invalid_tome_name(tome_name)
        src_name = (
            src_tome_name if src_tome_name is not None else self._default_header_name
        )
        return create_subheader_tome_from_fs(
            tome_name,
            src_tome_name=src_name,
            selector=selector,
            tome_collection_root_path=self._tome_collection_root_path,
            compression=compression,
            log=self._log,
        )

    def build_basic_tomes(
        self,
        channels: List[str | ChannelInstruction],
        /,
        *,
        tome_name: str = DEFAULT_TOME_NAME,
        header_tome_name: str = None,
        keys: List[str] = None,
        max_page_size_mb: float | None = DEFAULT_MAX_PAGE_SIZE_MB,
        compression: str | None = DEFAULT_PAGE_COMPRESSION,
        read_threads: int = DEFAULT_READ_THREADS,
        behavior_if_complete: str = "pass",
        behavior_if_partial: str = "overwrite",
    ) -> BasicTomes:
        """
        Build the header tome and one tome per channel, visiting each match once.

        The same tomes as `create_header_tome`, then for each channel a
        subheader of the matches that have it and a `make_tome` over that
        subheader with each match's rows tagged ``match_key``, but faster:
        each match is visited once, for its manifest, its ``header`` row and
        all of its channels, read with pyarrow.

        A channel's tome holds the matches that have the channel, in header
        order, and its header copy is their header rows. Nothing else is
        written: make any other subheader (all matches, one map, one
        platform) from the header with `create_subheader_tome`.

        Tomes are written to a staging folder, ``tome/<ds_type>/.building``,
        during the walk, and moved to their names at the end, once the
        header's dates are known. An interrupted build leaves no partial
        channel tome, and the next call starts over.

        Parameters
        ----------
        channels : list of str or ChannelInstruction
            The channels to build a tome for. An instruction,
            ``{"channel": ..., "columns": [...]}``, reads only those
            columns, as in `make_tome`.
        tome_name : str, default="basic_{channel}.{dates}"
            Name of each channel's tome. ``{channel}`` is the channel, and
            ``{dates}`` the first and last day of the header's
            ``match_date``, as ``yyyy-mm-dd,yyyy-mm-dd``.
        header_tome_name : str, default=`default_header_name`
            The header tome to write, or to use if it is complete.
        keys : list of str, default=every match in the ds collection
            Manifest keys of the matches to build over, as in a header
            tome's ``key`` column, when the header is built. A complete
            header that is kept must hold the same matches.
        max_page_size_mb : float or None, default=256
            A page is cut once its tables pass this many MB of Arrow
            buffers. That counts strings by their bytes, so a page holds
            more rows than a `make_tome` page of the same size, which counts
            pandas' in-memory size. Every channel fills a page at once, so
            memory peaks near this times the number of channels. None
            writes one page per tome.
        compression : str or None, default="zstd"
            Parquet codec for the pages, as in `make_tome`.
        read_threads : int, default=4
            Threads that read matches ahead of the writer. Tomes list the
            matches in header order whatever the count, and pages come out
            the same. 0 or 1 reads each match in turn. Reading from a hard
            disk, write the tomes to a different disk.
        behavior_if_complete : {"pass", "overwrite", "fail"}
            For a complete header or channel tome: keep it, build it again,
            or raise. Overwriting the header rescans the collection.
        behavior_if_partial : {"overwrite", "pass", "fail"}
            For a partial header or channel tome, such as one `make_tome`
            left: build it again, leave it, or raise. A partial header is
            built again unless "fail".

        Returns
        -------
        BasicTomes
            Loaders for the header and each channel's tome, and the
            ``{dates}`` value.

        Notes
        -----
        A complete tome is kept, so a call whose tomes are all complete
        reads no match. When the header is built, the channel tomes' names
        are known only at the end, so a "fail" for an existing channel tome
        is raised after the walk.

        A key listed twice, or declared by two manifests, is built once, and
        the count dropped is logged. Pages carry pandas metadata, so
        `TomeLoader` gives the dtypes a `make_tome` tome would: a column is
        ``Int64`` when any match's file declares it so. When pyarrow can't
        join a page's tables into those dtypes, that page is built with
        pandas.
        """
        header_name = (
            header_tome_name
            if header_tome_name is not None
            else self._default_header_name
        )
        return build_basic_tomes_from_fs(
            channels,
            tome_name=tome_name,
            header_tome_name=header_name,
            keys=keys,
            ds_type=self._ds_type,
            tome_collection_root_path=self._tome_collection_root_path,
            ds_collection_root_path=self._ds_collection_root_path,
            max_page_size_mb=max_page_size_mb,
            compression=compression,
            read_threads=read_threads,
            behavior_if_complete=behavior_if_complete,
            behavior_if_partial=behavior_if_partial,
            log=self._log,
        )

    @overload
    def get_dataframe(
        self,
        tome_name: str,
        *,
        columns: Optional[Sequence[str]] = None,
        library: Literal["pandas"] = "pandas",
        widen: bool = False,
    ) -> pd.DataFrame: ...

    @overload
    def get_dataframe(
        self,
        tome_name: str,
        *,
        columns: Optional[Sequence[str]] = None,
        library: Literal["polars"],
        widen: bool = False,
    ) -> "pl.DataFrame": ...

    def get_dataframe(self, tome_name, *, columns=None, library="pandas", widen=False):
        """
        Get the dataframe from a tome.

        Parameters
        ----------
        tome_name : str
            Name of the tome.
        columns : list of str, default=None
            Columns to read, in this order. None reads every column.
        library : {"pandas", "polars"}, default="pandas"
            "polars" returns a polars DataFrame, and needs the ``polars``
            extra (``pureskillgg-dsdk[polars]``).
        widen : bool, default=False
            Every integer column as a 64-bit integer and every float column
            as float64; bool columns stay bool (see
            `TomeLoader.get_dataframe`).

        Returns
        -------
        pd.DataFrame or pl.DataFrame
            The tome's data. A pandas frame has one ``RangeIndex``.
        """
        loader = self.get_loader(tome_name)
        return loader.get_dataframe(columns=columns, library=library, widen=widen)

    def scan(self, tome_name: str, *, widen: bool = False) -> "pl.LazyFrame":
        """
        Scan a tome as a polars LazyFrame. Needs the ``polars`` extra.

        Parameters
        ----------
        tome_name : str
            Name of the tome.
        widen : bool, default=False
            Integer columns as Int64 and float columns as Float64.

        Returns
        -------
        pl.LazyFrame
            The tome's data, read only as far as a query needs.
        """
        return self.get_loader(tome_name).scan(widen=widen)

    def get_keyset(self, tome_name: str) -> list:
        """
        Get the keyset from a tome.

        Parameters
        ----------
        tome_name : str
            Name of the tome.

        Returns
        -------
        list
            List containing the tome's keyset.
        """
        loader = self.get_loader(tome_name)
        return loader.get_keyset()

    def get_manifest(self, tome_name: str) -> dict:
        """
        Get the manifest from a tome.

        Parameters
        ----------
        tome_name : str
            Name of the tome.

        Returns
        -------
        dict
            Dictionary containing the tome's manifest.
        """
        loader = self.get_loader(tome_name)
        return loader.manifest

    def iterate_pages(self, tome_name: str) -> TomeLoader.iterate_pages:
        """
        Iterate through pages of a tome.

        Parameters
        ----------
        tome_name : str
            Name of the tome.

        Returns
        -------
        TomeLoader.iterate_pages
            Iterator for pages from TomeLoader.
        """
        loader = self.get_loader(tome_name)
        return loader.iterate_pages()

    def get_loader(self, tome_name: str) -> TomeLoader:
        """
        Get the loader for a tome.

        Parameters
        ----------
        tome_name : str
            Name of the tome.

        Returns
        -------
        TomeLoader
            The TomeLoader instance for this tome.
        """
        reader = TomeReaderFs(
            root_path=self._tome_collection_root_path,
            manifest_key="/".join(["tome", self._ds_type, tome_name, "tome"]),
            log=self._log,
        )
        loader = TomeLoader(reader=reader, log=self._log)
        return loader

    def get_header_loader(self) -> TomeLoader:
        """
        Get the loader for the header tome.

        Returns
        -------
        TomeLoader
            The TomeLoader instance for the header tome.
        """
        return self.get_loader(self._default_header_name)

    def get_random_match(self, subheader_name: str = None, /):
        """
        Get the data for a single random match.

        Parameters
        ----------
        subheader_name : str, default=from env (PURESKILLGG_TOME_DEFAULT_HEADER_NAME)
            Name of the subheader to use. Not specifying this will use the default header.

        Returns
        -------
        GameDsLoader
            The GameDsLoader instance for this tome.
        """
        loader = (
            self.get_header_loader()
            if subheader_name is None
            else self.get_loader(subheader_name)
        )

        keyset = loader.get_keyset()

        index = random.choice(range(0, len(keyset)))
        return self.get_match_by_index(index, subheader_name)

    def get_match_by_index(
        self, index: int = 0, subheader_name: str = None, /
    ) -> GameDsLoader:
        """
        Get the data for a single match.

        Parameters
        ----------
        index : int, default=0
            Index of the match to load.
        subheader_name : str, default=from env (PURESKILLGG_TOME_DEFAULT_HEADER_NAME)
            Name of the subheader to use. Not specifying this will use the default header.

        Returns
        -------
        GameDsLoader
            The GameDsLoader instance for this tome.
        """
        loader = (
            self.get_header_loader()
            if subheader_name is None
            else self.get_loader(subheader_name)
        )

        df_header = loader.get_dataframe()

        csds_reader = DsReaderFs(
            root_path=self._ds_collection_root_path,
            manifest_key=df_header["key"][index],
        )

        csds_loader = GameDsLoader(reader=csds_reader)

        return csds_loader

    # pylint: disable=too-many-locals
    def make_tome(
        self,
        tome_name: str,
        /,
        *,
        header_tome_name: str = None,
        ds_reading_instructions: List[ChannelInstruction] = None,
        max_page_size_mb: float = None,
        max_page_row_count: int = None,
        limit_check_frequency: int = 100,
        compression: str | None = DEFAULT_PAGE_COMPRESSION,
        **kwargs,
    ) -> TomeMaker:
        """
        Make a tome.

        Parameters
        ----------
        tome_name : str
            Name of the tome.
        ds_reading_instructions : ChannelInstruction, default=None
            Instructions on how to read in each DS file. Default value
            of None will read all channels and columns.
        max_page_size_mb : float, default = None
            Max size *in memory* that a tome can be. Generally it will be
            2-10x smaller on disk. By default the scribe will not check.
        max_page_row_count : int, default = None
            Max number of rows that a tome can be. By default it will
            not have a max row count.
        limit_check_frequency : int
            How often the scribe should check if it exceeded the max size or
            max row count. This should be a multiple of the print frequency
            which is set to 100.
        compression : str or None, default="zstd"
            Parquet codec for the pages this call writes, including the
            tome's copy of its header. Any codec `DataFrame.to_parquet`
            accepts: ``"gzip"`` writes pages in the format dsdk 3.2.2 and
            earlier used, and ``None`` writes them uncompressed. Pages
            already written are left as they are, so continuing a gzip tome
            with the default adds zstd pages; the tome still reads whole.
        **kwargs:
            Keywords passed through to the TomeMaker.

        Returns
        -------
        TomeMaker
            The TomeMaker instance to make a tome.
        """
        warn_if_invalid_tome_name(tome_name)
        header_name = (
            header_tome_name
            if header_tome_name is not None
            else self._default_header_name
        )
        name = tome_name

        existing_tome_loader = self.get_loader(name)

        header_loader = self.get_loader(header_name)
        writer = TomeWriterFs(
            root_path=self._tome_collection_root_path,
            compression=compression,
            log=self._log,
        )
        manifest = TomeManifest(
            tome_name=name,
            ds_type=self._ds_type,
            header_tome_name=header_name,
            log=self._log,
        )
        scribe = TomeScribe(
            manifest=manifest,
            writer=writer,
            max_page_size_mb=max_page_size_mb,
            max_page_row_count=max_page_row_count,
            limit_check_frequency=limit_check_frequency,
            log=self._log,
        )
        header_copier = HeaderTomeCopierFs(
            src_tome_name=header_name,
            tome_collection_root_path=self._tome_collection_root_path,
            dest_tome_name=name,
            ds_type=self._ds_type,
            compression=compression,
            log=self._log,
        )

        tomer = TomeMaker(
            header_loader=header_loader,
            scribe=scribe,
            ds_reading_instructions=ds_reading_instructions,
            ds_type=self._ds_type,
            ds_collection_root_path=self._ds_collection_root_path,
            tome_loader=existing_tome_loader,
            header_copier=header_copier,
            **kwargs,
            log=self._log,
        )

        return tomer


def get_env_option(name, value):
    if value is not None:
        return value
    env_prefix = "pureskillgg_tome"
    key = "_".join([env_prefix, name]).upper()
    env_value = os.environ.get(key)
    if env_value is not None:
        return env_value
    raise Exception(f"Missing option {name}, pass in or set {key} in environment")
