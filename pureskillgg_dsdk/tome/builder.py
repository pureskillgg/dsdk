"""
Build a header tome and one tome per channel in one walk over the matches.

``make_tome`` reads one channel of a header's matches per call, and each call
reads every match's manifest again. ``build_basic_tomes_from_fs`` visits each
match once: it reads the manifest, the header row and every requested
channel together, with pyarrow, and pages each channel's rows into its own
tome.
"""

# pylint: disable=too-many-lines

import functools
import json
import os
import shutil
import string
import time
import warnings
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from itertools import islice

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import structlog

from ..ds_io import DsReaderFs
from ..ds_io.normalize_instructions import normalize_instructions
from .constants import (
    DEFAULT_PAGE_COMPRESSION,
    MATCH_KEY_COLUMN,
    filter_ds_reader_logs,
    get_page_path_fs,
    make_key,
    warn_if_invalid_tome_name,
)
from .header_tome import create_subheader_tome_from_fs, get_manifest_key_paths_from_glob
from .loader import TomeLoader
from .manifest import TomeManifest
from .narrowing import (
    is_wide,
    missing_as_nullable,
    narrow_table,
    plan_narrowing,
    resolved_narrow_int,
    type_name,
)
from .reader_fs import TomeReaderFs
from .writer_fs import TomeWriterFs, ensure_dir

DEFAULT_TOME_NAME = "basic_{channel}.{dates}"
DEFAULT_MAX_PAGE_SIZE_MB = 256
DEFAULT_READ_THREADS = 4
# The header tome's rows are keyed by this column, a channel tome's by match_key.
HEADER_KEY_COLUMN = "key"
BEHAVIORS_IF_COMPLETE = ("pass", "overwrite", "fail")
BEHAVIORS_IF_PARTIAL = ("overwrite", "pass", "fail")
# Tomes are written here during the walk, and moved to their names at the end.
STAGING_FOLDER = ".building"

_INDEX_COLUMN = "__index_level_0__"
_PARQUET = "application/x-parquet"
# A page's buffered tables are merged every this many, so a page of many small
# matches doesn't hold tens of thousands of small tables.
_COMPACT_EVERY = 256
_STATUS_EVERY = 1000
_ARROW_ERRORS = (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError)


@dataclass(frozen=True)
class BasicTomes:
    """
    The tomes `build_basic_tomes` wrote or found.

    Attributes
    ----------
    header : TomeLoader
        The header tome: one row per match.
    tomes : dict of str to TomeLoader
        Each channel's tome, by channel name. A channel that no match has
        gets no tome and is left out.
    dates : str or None
        The ``{dates}`` value for tome names: the first and last day of the
        header's ``match_date``, as ``yyyy-mm-dd,yyyy-mm-dd``. None when the
        header has no ``match_date``.
    """

    header: TomeLoader
    tomes: dict
    dates: str | None


def build_basic_tomes_from_fs(
    channels,
    /,
    *,
    tome_name=DEFAULT_TOME_NAME,
    header_tome_name="header",
    keys=None,
    ds_type="csds",
    tome_collection_root_path="tomes",
    ds_collection_root_path="data",
    max_page_size_mb=DEFAULT_MAX_PAGE_SIZE_MB,
    compression=DEFAULT_PAGE_COMPRESSION,
    read_threads=DEFAULT_READ_THREADS,
    behavior_if_complete="pass",
    behavior_if_partial="overwrite",
    log=None,
) -> BasicTomes:
    """
    Build a header tome and one tome per channel in one walk over the matches.

    `TomeCuratorFs.build_basic_tomes` documents the arguments; this function
    takes the curator's paths and ``ds_type`` as keywords.
    """
    return _Build(
        channels,
        tome_name=tome_name,
        header_tome_name=header_tome_name,
        keys=keys,
        ds_type=ds_type,
        tome_root=tome_collection_root_path,
        ds_root=ds_collection_root_path,
        max_page_size_mb=max_page_size_mb,
        compression=compression,
        read_threads=read_threads,
        behaviors=(behavior_if_complete, behavior_if_partial),
        log=log,
    ).run()


class _Build:
    def __init__(
        self,
        channels,
        *,
        tome_name,
        header_tome_name,
        keys,
        ds_type,
        tome_root,
        ds_root,
        max_page_size_mb,
        compression,
        read_threads,
        behaviors,
        log,
    ):
        self._columns = normalize_channels(channels)
        check_tome_name(tome_name)
        check_behaviors(*behaviors)
        if read_threads < 0:
            raise ValueError(f"read_threads must be 0 or more, not {read_threads}")
        if max_page_size_mb is not None and max_page_size_mb <= 0:
            raise ValueError(
                f"max_page_size_mb must be positive or None, not {max_page_size_mb}"
            )
        warn_if_invalid_tome_name(header_tome_name)
        if "{dates}" not in tome_name:
            # The names don't wait for the header: check them before reading.
            tome_names(tome_name, self._columns, None, header_tome_name)

        self._tome_name = tome_name
        self._header_name = header_tome_name
        self._keys = keys
        self._ds_type = ds_type
        self._tome_root = tome_root
        self._ds_root = ds_root
        self._max_page_size_mb = max_page_size_mb
        self._compression = compression
        self._threads = read_threads
        self._if_complete, self._if_partial = behaviors
        self._log = structlog.wrap_logger(
            log if log is not None else structlog.get_logger(),
            processors=[filter_ds_reader_logs],
        )
        # Fails now on a codec parquet can't write.
        self._writer = TomeWriterFs(
            root_path=tome_root, compression=compression, log=self._log
        )
        self._proxies = _Proxies()
        self._header_keys = []

    def run(self) -> BasicTomes:
        keep_header = self._keep_header()
        header = None
        if keep_header:
            paths = self._load_header()
            dates = header_dates(self._loader(self._header_name).get_dataframe())
            names = tome_names(self._tome_name, self._columns, dates, self._header_name)
            channels = [c for c, name in names.items() if self._build_tome(name)]
        else:
            paths = self._header_paths()
            channels = list(self._columns)
            header = self._scribe(self._header_name, is_header=True)
        scribes = {c: self._scribe(self._staging_name(c)) for c in channels}
        if header is not None or len(scribes) > 0:
            self._walk(paths, header, scribes)
        if header is not None:
            header.finish()
            dates = header_dates(self._loader(self._header_name).get_dataframe())
            names = tome_names(self._tome_name, self._columns, dates, self._header_name)
            scribes = {c: s for c, s in scribes.items() if self._build_tome(names[c])}
        for channel, scribe in scribes.items():
            self._publish(channel, scribe, names[channel])
        self._remove_staging()
        tomes = {}
        for channel, name in names.items():
            loader = self._loader(name)
            if loader.exists:
                tomes[channel] = loader
        self._log.info(
            "Build Basic Tomes Done",
            matches=len(self._header_keys),
            tomes_written=sum(s.key_count > 0 for s in scribes.values()),
            pages_built_with_pandas=sum(s.pandas_pages for s in scribes.values()),
        )
        return BasicTomes(
            header=self._loader(self._header_name), tomes=tomes, dates=dates
        )

    # Which tomes to build.

    def _keep_header(self) -> bool:
        loader = self._loader(self._header_name)
        if not loader.exists:
            return False
        complete = loader.is_complete
        behavior = self._if_complete if complete else self._if_partial
        if behavior == "fail":
            raise Exception(f"Tome already exists {self._header_name}")
        # A partial header is from an interrupted build: it is built again.
        return complete and behavior == "pass"

    def _build_tome(self, name) -> bool:
        warn_if_invalid_tome_name(name)
        loader = self._loader(name)
        if not loader.exists:
            return True
        behavior = self._if_complete if loader.is_complete else self._if_partial
        if behavior == "fail":
            raise Exception(f"Tome already exists {name}")
        return behavior == "overwrite"

    def _load_header(self):
        """The matches of the kept header tome."""
        loader = self._loader(self._header_name)
        keys, dropped = dedupe(loader.get_keyset())
        self._log_duplicates(dropped)
        if self._keys is not None:
            given, _ = dedupe(normalize_key(key) for key in self._keys)
            if set(given) != set(keys):
                raise ValueError(
                    f"keys differ from the matches of the complete header tome "
                    f"{self._header_name}; pass behavior_if_complete='overwrite' "
                    f"to rebuild it, or another header_tome_name"
                )
        self._header_keys = keys
        return keys

    def _header_paths(self):
        """The manifests to build the header from."""
        if self._keys is None:
            paths = get_manifest_key_paths_from_glob(self._ds_root, self._ds_type)
        else:
            paths, dropped = dedupe(normalize_key(key) for key in self._keys)
            self._log_duplicates(dropped)
        if len(paths) == 0:
            raise ValueError(f"No matches to build a header from in {self._ds_root}")
        return paths

    # The walk.

    def _walk(self, paths, header, scribes):
        """Visit each match once: manifest, header row and channels."""
        # What an interrupted build left is started over.
        self._remove_staging()
        channels = list(scribes)
        for scribe in scribes.values():
            scribe.start()
        if header is not None:
            header.start()
        progress = _Progress(self._log, len(paths))
        seen = set()
        duplicates = 0

        def read(path):
            return self._read_match(path, channels, with_header=header is not None)

        for key, row, tables in _ordered_map(read, paths, self._threads):
            progress.step()
            if key in seen:
                duplicates += 1
                continue
            seen.add(key)
            if header is not None:
                self._header_keys.append(key)
                header.add(key, row)
            for channel, table in tables:
                scribes[channel].add(key, table)
        self._log_duplicates(duplicates)

    def _read_match(self, path, channels, *, with_header):
        manifest = DsReaderFs(
            root_path=self._ds_root, manifest_key=path, log=self._log
        ).read_manifest()
        by_name = {entry["channel"]: entry for entry in manifest["channels"]}
        key = path
        row = None
        if with_header:
            # The header's key is the one the manifest declares, as in
            # create_header_tome; a manifest without one keys by its path.
            key = manifest.get("key") or path.replace(os.sep, "/")
            if "header" not in by_name:
                raise Exception("Channel header not found in replay.")
            table = read_channel(self._channel_path(by_name["header"]), None, "header")
            row = prepare_table(
                table, [(HEADER_KEY_COLUMN, key), ("match_id", manifest["id"])]
            )
        tables = []
        for channel in channels:
            if channel not in by_name:
                continue
            file_path = self._channel_path(by_name[channel])
            table = read_channel(file_path, self._columns[channel], channel)
            tables.append((channel, prepare_table(table, [(MATCH_KEY_COLUMN, key)])))
        return key, row, tables

    def _channel_path(self, entry) -> str:
        content_type = entry["contentType"]
        if content_type != _PARQUET:
            raise Exception(f"Unsupported content type {content_type}")
        return os.path.join(self._ds_root, os.path.normpath(entry["key"]))

    # Writing the tomes.

    def _scribe(self, name, *, is_header=False):
        return _ArrowScribe(
            manifest=TomeManifest(
                tome_name=name,
                ds_type=self._ds_type,
                is_header=is_header,
                header_tome_name=None if is_header else self._header_name,
                log=self._log,
            ),
            writer=self._writer,
            # A header tome is one page, as create_header_tome writes it.
            max_page_size_mb=None if is_header else self._max_page_size_mb,
            proxies=self._proxies,
            key_column=HEADER_KEY_COLUMN if is_header else MATCH_KEY_COLUMN,
            tome_root=self._tome_root,
            log=self._log,
        )

    def _staging_name(self, channel) -> str:
        return "/".join([STAGING_FOLDER, self._header_name, channel])

    def _staging_path(self) -> str:
        return os.path.join(
            self._tome_root, "tome", self._ds_type, STAGING_FOLDER, self._header_name
        )

    def _remove_staging(self):
        path = self._staging_path()
        shutil.rmtree(path, ignore_errors=True)
        try:
            # The staging folder itself goes when no other build uses it.
            os.rmdir(os.path.dirname(path))
        except OSError:
            pass

    def _publish(self, channel, scribe, name):
        """Move a channel's staged tome to its name, with its header copy."""
        if scribe.key_count == 0:
            self._log.info("Channel in no matches: no tome", channel=channel)
            return
        scribe.finish()
        manifest = scribe.manifest
        final = dict(
            manifest, tome=name, key=make_key(["tome", self._ds_type, name, "tome"])
        )
        # Mark the destination partial, with no pages, before any page file
        # there is replaced: an interrupted publish leaves a partial tome,
        # which the next build builds again, never a complete tome whose
        # pages are half old and half new.
        self._writer.write_manifest(dict(final, isComplete=False, pages=[]))
        pages = []
        for page in manifest["pages"]:
            moved = dict(page)
            for subtype in ("dataframe", "keyset"):
                key = make_key(["tome", self._ds_type, name, page[subtype][subtype]])
                destination = os.path.join(self._tome_root, key)
                ensure_dir(destination)
                os.replace(
                    get_page_path_fs(self._tome_root, subtype, page), destination
                )
                moved[subtype] = dict(page[subtype], key=key)
            pages.append(moved)
        self._copy_header(name, scribe.keys)
        self._writer.write_manifest(dict(final, pages=pages))

    def _copy_header(self, name, keys):
        """The tome's header copy: the header rows of the matches it holds."""
        keys = frozenset(keys)
        create_subheader_tome_from_fs(
            name,
            src_tome_name=self._header_name,
            selector=lambda df: df["key"].isin(keys) & ~df["key"].duplicated(),
            tome_collection_root_path=self._tome_root,
            ds_type=self._ds_type,
            is_copied_header=True,
            preserve_src_id=True,
            compression=self._compression,
            log=self._log,
        )

    def _loader(self, name) -> TomeLoader:
        reader = TomeReaderFs(
            root_path=self._tome_root,
            manifest_key=make_key(["tome", self._ds_type, name, "tome"]),
            log=self._log,
        )
        return TomeLoader(reader=reader, log=self._log)

    def _log_duplicates(self, count):
        if count > 0:
            self._log.warning("Duplicate match keys dropped", count=count)


class _ArrowScribe:
    """
    Pages of pyarrow tables for one tome: the TomeScribe of the builder.

    A page is cut when its buffered tables pass ``max_page_size_mb`` of Arrow
    buffers (``Table.nbytes``), checked after every match.

    When the matches mix wide and narrow column types (see `narrowing`), each
    page is narrowed as it is built. A page written before the narrow types
    turned up is narrowed and written again when the tome is finished, so
    every page holds the tome's types.
    """

    def __init__(
        self,
        *,
        manifest,
        writer,
        max_page_size_mb,
        proxies,
        key_column,
        tome_root,
        log,
    ):
        self._manifest = manifest
        self._writer = writer
        self._max_bytes = (
            None if max_page_size_mb is None else max_page_size_mb * 1024 * 1024
        )
        self._proxies = proxies
        self._key_column = key_column
        self._tome_root = tome_root
        self._log = log
        self._page_counter = 0
        self._page = _Page(proxies, key_column)
        # Every table signature the tome holds, and each written page's keys.
        self._signatures = set()
        self._page_keys = []
        self.keys = []
        self.pandas_pages = 0
        self.narrowed_pages = 0

    @property
    def key_count(self) -> int:
        return len(self.keys)

    @property
    def manifest(self) -> dict:
        return self._manifest.get()

    def start(self):
        self._writer.write_manifest(self._manifest.get())
        self._manifest.start_page()

    def add(self, key, prepared):
        """Add one match: its key, and its rows as (table, signature) or None."""
        self.keys.append(key)
        if prepared is not None:
            self._signatures.add(prepared[1])
        self._page.add(key, prepared)
        if self._max_bytes is not None and self._page.nbytes > self._max_bytes:
            self._write()

    def finish(self):
        if len(self.keys) == 0:
            raise Exception("Empty Tome not supported")
        if len(self._page.keys) > 0:
            self._write()
        self._narrow_written_pages()
        self._manifest.finish()
        self._writer.write_manifest(self._manifest.get())

    def _write(self):
        data = self._page.table()
        if data is None:
            data = self._page.frame()
            if len(self._page.items) > 0:
                self.pandas_pages += 1
                self._log.info(
                    "Page built with pandas",
                    tome=self._manifest.get()["tome"],
                    page_number=self._page_counter,
                )
        page = self._manifest.end_page(self._page_counter)
        self._writer.write_page(page, data, self._page.keys)
        self._writer.write_manifest(self._manifest.get())
        self._page_keys.append(self._page.keys)
        self._page_counter += 1
        self._page = _Page(self._proxies, self._key_column)
        self._manifest.start_page()

    def _narrow_written_pages(self):
        """
        Narrow, and write again, the pages that still hold a column wide that
        the tome narrows: pages written before a match with the narrow type.
        """
        plan = plan_narrowing(signature[0] for signature in self._signatures)
        if not plan:
            return
        for page, keys in zip(self._manifest.get()["pages"], self._page_keys):
            path = get_page_path_fs(self._tome_root, "dataframe", page)
            schema = pq.read_schema(path)
            wide = {
                name: target
                for name, target in plan.items()
                if name in schema.names and is_wide(schema.field(name).type)
            }
            if not wide:
                continue
            table = pq.read_table(path)
            table = narrow_table(table, wide, describe_rows(table, self._key_column))
            self._writer.write_page(page, table, keys)
            self.narrowed_pages += 1
            self._log.info(
                "Page narrowed",
                tome=self._manifest.get()["tome"],
                page_number=page["number"],
                columns=sorted(wide),
            )


class _Page:
    """The tables and keys of the page being filled."""

    def __init__(self, proxies, key_column):
        self._proxies = proxies
        self._key_column = key_column
        self.keys = []
        self._blocks = []  # merged tables, their pandas dtypes resolved
        self._block_bytes = 0
        self._tail = []  # tables as read
        self._tail_bytes = 0
        self._tail_signatures = set()
        self._signatures = set()
        self._pandas_only = False

    @property
    def items(self) -> list:
        return self._blocks + self._tail

    @property
    def nbytes(self) -> int:
        return self._block_bytes + self._tail_bytes

    def add(self, key, prepared):
        self.keys.append(key)
        if prepared is None:
            return
        table, table_signature = prepared
        self._proxies.add(table_signature, table)
        self._tail.append(table)
        self._tail_bytes += table.nbytes
        self._tail_signatures.add(table_signature)
        self._signatures.add(table_signature)
        if len(self._tail) >= _COMPACT_EVERY:
            self._compact()

    def table(self):
        """The page as one table, or None when pandas must build it."""
        if len(self.items) == 0 or self._pandas_only:
            return None
        return realize(self.items, self._signatures, self._proxies, self._key_column)

    def frame(self):
        """The page built with pandas, as make_tome builds it, but narrowed."""
        return pandas_page(self.items, self._signatures, self._key_column)

    def _compact(self):
        if self._pandas_only:
            return
        block = realize(
            self._tail, self._tail_signatures, self._proxies, self._key_column
        )
        if block is None:
            # pandas will build the page; keep the tables as read.
            self._pandas_only = True
            return
        block = block.combine_chunks()
        self._blocks.append(block)
        self._block_bytes += block.nbytes
        self._tail, self._tail_bytes, self._tail_signatures = [], 0, set()


class _Progress:
    def __init__(self, log, total):
        self._log = log
        self._total = total
        self._done = 0
        self._start = time.time()

    def step(self):
        self._done += 1
        if self._done % _STATUS_EVERY != 0 or self._done >= self._total:
            return
        elapsed = time.time() - self._start
        remaining = elapsed * (self._total - self._done) / self._done
        self._log.info(
            "Build Basic Tomes Update",
            matches_done=self._done,
            matches_total=self._total,
            percent_complete=round(100 * self._done / self._total, 3),
            minutes_elapsed=int(elapsed / 60),
            minutes_remaining=int(remaining / 60),
        )


def read_channel(path, columns, channel) -> pa.Table:
    try:
        return pq.read_table(path, columns=columns)
    except ValueError as err:
        raise Exception(f"Couldn't read in parquet file {channel}") from err


def prepare_table(table, columns):
    """
    A channel table ready for a page, as (table, signature), or None if empty.

    Drops the stored pandas index (``make_tome`` pages discard it too),
    decodes dictionary columns to their values, and sets each of
    ``columns``, (name, string value) pairs, on every row, in place when the
    column exists, as ``df[name] = value`` would.

    A dictionary column (a pandas category, such as ``place_name`` in the
    compact player_status) is decoded so that pyarrow can join it with the
    same column of other matches, which hold other dictionaries or plain
    strings. pandas reads it as strings, as ``pd.concat`` gives categories
    that differ between matches.
    """
    if table.num_rows == 0:
        return None
    raw = (table.schema.metadata or {}).get(b"pandas")
    index = index_columns(raw)
    drop = [n for n in table.column_names if n == _INDEX_COLUMN or n in index]
    if drop:
        table = table.drop_columns(drop)
    table, decoded = decode_dictionaries(table)
    for name, value in columns:
        column = pa.repeat(pa.scalar(value, pa.string()), table.num_rows)
        if name in table.column_names:
            table = table.set_column(table.column_names.index(name), name, column)
        else:
            table = table.append_column(name, column)
    metadata = prepared_metadata(
        raw, tuple(table.column_names), tuple(name for name, _ in columns), decoded
    )
    table = table.replace_schema_metadata(
        None if metadata is None else {b"pandas": metadata}
    )
    return table, dtype_signature(table, metadata)


def decode_dictionaries(table):
    """
    The table with each dictionary column cast to its value type, and the
    decoded columns as (name, pandas_type, numpy_type) for their metadata.
    """
    decoded = []
    for position, field in enumerate(table.schema):
        if not pa.types.is_dictionary(field.type):
            continue
        value_type = field.type.value_type
        table = table.set_column(
            position, field.name, table.column(position).cast(value_type)
        )
        if pa.types.is_string(value_type) or pa.types.is_large_string(value_type):
            decoded.append((field.name, "unicode", "object"))
        else:
            decoded.append(
                (
                    field.name,
                    type_name(value_type),
                    str(np.dtype(value_type.to_pandas_dtype())),
                )
            )
    return table, tuple(decoded)


@functools.lru_cache(maxsize=1024)
def index_columns(raw) -> frozenset:
    if raw is None:
        return frozenset()
    return frozenset(
        c for c in json.loads(raw).get("index_columns", []) if isinstance(c, str)
    )


@functools.lru_cache(maxsize=1024)
def prepared_metadata(raw, names, added, decoded=()):
    """
    A file's pandas metadata for the columns kept and added, without index.
    A decoded dictionary column, (name, pandas_type, numpy_type), gets an
    entry for its values in place of its category entry.
    """
    if raw is None:
        return None
    meta = json.loads(raw)
    kept = set(names) - set(added)
    values = {
        name: (pandas_type, numpy_type) for name, pandas_type, numpy_type in decoded
    }
    columns = []
    for column in meta.get("columns", []):
        name = field_name(column)
        if name not in kept:
            continue
        if name in values:
            pandas_type, numpy_type = values[name]
            column = {
                "name": column.get("name", name),
                "field_name": name,
                "pandas_type": pandas_type,
                "numpy_type": numpy_type,
                "metadata": None,
            }
        columns.append(column)
    for name in added:
        columns.append(
            {
                "name": name,
                "field_name": name,
                "pandas_type": "unicode",
                "numpy_type": "object",
                "metadata": None,
            }
        )
    meta["columns"] = columns
    meta["index_columns"] = []
    return json.dumps(meta).encode("utf8")


def field_name(column_metadata) -> str:
    return column_metadata.get("field_name", column_metadata.get("name"))


def dtype_signature(table, metadata) -> tuple:
    """What decides a table's pandas dtypes: types, pandas metadata, nulls."""
    nulls = tuple(
        0 if c.null_count == 0 else (2 if c.null_count == len(c) else 1)
        for c in table.columns
    )
    # A Schema's hash and equality leave its metadata out, so it's added.
    return (table.schema, metadata, nulls)


def proxy_table(table) -> pa.Table:
    """
    Two rows per column that give the dtypes pandas reads the table with.

    A column keeps its type and pandas metadata, and whether it has no
    nulls, some, or only nulls: that is all ``to_pandas`` and ``pd.concat``
    look at to choose a dtype.
    """
    columns = []
    for column in table.columns:
        nulls = column.null_count
        if nulls in (0, len(column)):
            indices = [0, 0]
        else:
            first = pc.index(column.is_valid(), True).as_py()
            indices = [first, None]
        columns.append(column.take(pa.array(indices, pa.int64())))
    return pa.Table.from_arrays(columns, schema=table.schema)


def proxy_frame(table) -> pd.DataFrame:
    """The two-row stand-in of a table, read by pandas."""
    return proxy_table(table).to_pandas()


class _Proxies:
    """
    The two-row stand-in of each distinct table signature, and the pandas
    frames they give as they are or narrowed by a plan.
    """

    def __init__(self):
        self._tables = {}
        self._frames = {}

    def add(self, signature, table):
        if signature not in self._tables:
            self._tables[signature] = proxy_table(table)

    def frames(self, signatures, plan) -> list:
        frames = []
        for signature in signatures:
            names = signature[0].names
            relevant = tuple(
                sorted(
                    ((n, t) for n, t in plan.items() if n in names),
                    key=lambda item: item[0],
                )
            )
            key = (signature, relevant)
            if key not in self._frames:
                table = narrow_table(
                    self._tables[signature], dict(relevant), check=False
                )
                self._frames[key] = table.to_pandas()
            frames.append(self._frames[key])
        return frames


def describe_rows(table, key_column):
    """Name a row of a table for a NarrowingError: by its match key if it has one."""

    def describe(row):
        if key_column in table.column_names:
            return f"match {table.column(key_column)[row].as_py()!r}"
        return f"row {row}"

    return describe


def narrow_items(items, signatures, key_column):
    """The page's tables, narrowed where its matches mix wide and narrow types."""
    plan = plan_narrowing(signature[0] for signature in signatures)
    if not plan:
        return items, plan
    narrowed = [
        narrow_table(item, plan, describe_rows(item, key_column)) for item in items
    ]
    return narrowed, plan


def page_dtypes(frames) -> dict:
    """The dtypes pd.concat gives these frames, as make_tome's page would get."""
    with warnings.catch_warnings():
        # pandas 2 warns that all-NA columns will count in a future version.
        warnings.simplefilter("ignore", FutureWarning)
        return dict(pd.concat(frames, ignore_index=True).dtypes)


def realize(items, signatures, proxies, key_column=MATCH_KEY_COLUMN):
    """
    Concatenate tables into one whose pandas dtypes are make_tome's.

    pyarrow promotes differing types (int64 with double, null with any) and
    fills missing columns with nulls. The pandas metadata then names the
    dtype pd.concat gives the same matches, so a column is Int64 when any
    match declares Int64. Returns None when pyarrow can't concatenate the
    tables or the result doesn't read back as those dtypes; the caller then
    builds the page with pandas.

    Where the matches mix wide and narrow types, the wide columns are first
    narrowed (see `narrowing`), and a narrow integer column with missing
    values is pandas' nullable type of its width instead of float64.
    """
    items, plan = narrow_items(items, signatures, key_column)
    try:
        table = pa.concat_tables(items, promote_options="permissive")
    except _ARROW_ERRORS:
        return None
    expected = page_dtypes(proxies.frames(signatures, plan))
    if set(expected) != set(table.column_names):
        return None
    expected.update(missing_as_nullable(expected, narrow_int_columns(table.schema)))
    table = table.replace_schema_metadata(
        {b"pandas": page_metadata(items, table.schema, expected)}
    )
    try:
        actual = dict(proxy_frame(table).dtypes)
    except _ARROW_ERRORS:
        return None
    if actual != expected:
        return None
    return table


def page_metadata(items, schema, dtypes) -> bytes:
    """pandas metadata for a page: the files' entries, with the page's dtypes."""
    base = None
    entries = {}
    for item in items:
        raw = (item.schema.metadata or {}).get(b"pandas")
        if raw is None:
            continue
        meta = json.loads(raw)
        if base is None:
            base = meta
        for column in meta.get("columns", []):
            entries.setdefault(field_name(column), column)
        if all(name in entries for name in schema.names):
            break
    columns = []
    for name in schema.names:
        entry = dict(
            entries.get(name)
            or {"name": name, "field_name": name, "pandas_type": "object"}
        )
        entry["numpy_type"] = str(dtypes[name])
        entry.setdefault("metadata", None)
        columns.append(entry)
    base = base or {}
    meta = {
        "index_columns": [],
        "column_indexes": base.get("column_indexes", []),
        "columns": columns,
        "creator": base.get(
            "creator", {"library": "pyarrow", "version": pa.__version__}
        ),
        "pandas_version": pd.__version__,
    }
    return json.dumps(meta).encode("utf8")


def narrow_int_columns(schema) -> dict:
    """The columns of a joined table that are narrow integers, with their type."""
    return {
        field.name: field.type
        for field in schema
        if resolved_narrow_int([field.type]) is not None
    }


def pandas_page(items, signatures=(), key_column=MATCH_KEY_COLUMN) -> pd.DataFrame:
    """
    The page as make_tome builds it: pd.concat of the matches' frames, after
    narrowing as `realize` does.
    """
    if len(items) == 0:
        return pd.DataFrame()
    items, _ = narrow_items(items, signatures, key_column)
    frame = pd.concat([item.to_pandas() for item in items], ignore_index=True)
    types = {}
    for item in items:
        for field in item.schema:
            types.setdefault(field.name, []).append(field.type)
    narrow_ints = {}
    for name, column_types in types.items():
        data_type = resolved_narrow_int(column_types)
        if data_type is not None:
            narrow_ints[name] = data_type
    for name, dtype in missing_as_nullable(dict(frame.dtypes), narrow_ints).items():
        frame[name] = frame[name].astype(dtype)
    return frame


def _ordered_map(function, items, threads):
    """Yield function(item) in order; with 2 or more threads, read ahead."""
    if threads <= 1:
        for item in items:
            yield function(item)
        return
    pool = ThreadPoolExecutor(max_workers=threads)
    try:
        items = iter(items)
        pending = deque(pool.submit(function, i) for i in islice(items, 2 * threads))
        while pending:
            result = pending.popleft().result()
            for item in islice(items, 1):
                pending.append(pool.submit(function, item))
            yield result
    finally:
        pool.shutdown(wait=True, cancel_futures=True)


def normalize_channels(channels) -> dict:
    """Channel name -> columns to read (None for all), in the order given."""
    if isinstance(channels, (str, dict)):
        raise ValueError("channels must be a list of channel names or instructions")
    instructions = [{"channel": c} if isinstance(c, str) else dict(c) for c in channels]
    if len(instructions) == 0:
        raise ValueError("No channels to build")
    columns = {
        i["channel"]: i.get("columns") for i in normalize_instructions(instructions)
    }
    order = dict.fromkeys(i["channel"] for i in instructions)
    return {channel: columns[channel] for channel in order}


def check_tome_name(tome_name):
    fields = {
        field
        for _, field, _, _ in string.Formatter().parse(tome_name)
        if field is not None
    }
    if "channel" not in fields:
        raise ValueError(f"tome_name must contain {{channel}}: {tome_name!r}")
    unknown = fields - {"channel", "dates"}
    if unknown:
        raise ValueError(
            f"tome_name may only use {{channel}} and {{dates}}, not {sorted(unknown)}"
        )


def check_behaviors(if_complete, if_partial):
    for name, behavior, allowed in [
        ("behavior_if_complete", if_complete, BEHAVIORS_IF_COMPLETE),
        ("behavior_if_partial", if_partial, BEHAVIORS_IF_PARTIAL),
    ]:
        if behavior not in allowed:
            raise ValueError(f"{name} must be one of {allowed}, not {behavior!r}")


def tome_names(tome_name, channels, dates, header_name) -> dict:
    if dates is None and "{dates}" in tome_name:
        raise ValueError("tome_name uses {dates}, but the header has no match_date")
    names = {c: tome_name.format(channel=c, dates=dates) for c in channels}
    for channel, name in names.items():
        if name == header_name:
            raise ValueError(
                f"The tome of channel {channel} would be the header tome {name}"
            )
    return names


def header_dates(header) -> str | None:
    if "match_date" not in header.columns:
        return None
    dates = header["match_date"].dropna().astype(str).str[:10]
    if len(dates) == 0:
        return None
    return f"{dates.min()},{dates.max()}"


def dedupe(keys):
    """The keys without repeats, in order, and how many repeats were dropped."""
    seen = {}
    total = 0
    for key in keys:
        total += 1
        seen.setdefault(key, None)
    return list(seen), total - len(seen)


def ordered(keys, wanted):
    """The wanted keys in the order of keys, then any others."""
    wanted = set(wanted)
    first = [k for k in keys if k in wanted]
    rest = wanted.difference(first)
    return first + sorted(rest)


def normalize_key(key) -> str:
    return key.replace("\\", "/")
