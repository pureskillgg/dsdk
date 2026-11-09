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
from .incremental import (
    RESUME_FIELD,
    SIGNATURES_FIELD,
    decode_schemas,
    encode_schemas,
    next_page_number,
    read_tome_state,
    remove_orphans,
    tome_schemas,
)
from .loader import TomeLoader
from .manifest import TomeManifest, now_to_iso
from .narrowing import (
    NarrowingError,
    flag_columns,
    missing_as_nullable,
    narrow_table,
    plan_narrowing,
    resolved_narrow_int,
    type_name,
)
from .page_frames import read_pages_pandas
from .reader_fs import TomeReaderFs
from .writer_fs import TomeWriterFs, ensure_dir, replace_file

DEFAULT_TOME_NAME = "basic_{channel}.{dates}"
DEFAULT_MAX_PAGE_SIZE_MB = 256
DEFAULT_READ_THREADS = 4
# The header tome's rows are keyed by this column, a channel tome's by match_key.
HEADER_KEY_COLUMN = "key"
BEHAVIORS_IF_COMPLETE = ("pass", "overwrite", "fail", "extend")
BEHAVIORS_IF_PARTIAL = ("overwrite", "pass", "fail")
# Tomes are written here during the walk, and moved to their names at the end.
STAGING_FOLDER = ".building"
# The staged header of an extension: no channel is called this.
_STAGED_HEADER = "__header__"

_INDEX_COLUMN = "__index_level_0__"
_PARQUET = "application/x-parquet"
# A page's buffered tables are merged every this many, so a page of many small
# matches doesn't hold tens of thousands of small tables.
_COMPACT_EVERY = 256
_STATUS_EVERY = 1000
_ARROW_ERRORS = (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError)
# Narrow types a later match may still widen to int16 or int32.
_MAY_WIDEN = (pa.int8(), pa.int16())


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
    resume=False,
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
        resume=resume,
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
        resume=False,
        log,
    ):
        self._columns = normalize_channels(channels)
        check_tome_name(tome_name)
        check_behaviors(*behaviors)
        if behaviors[0] == "extend" and "{dates}" in tome_name:
            raise ValueError(
                f"behavior_if_complete='extend' needs a tome_name without {{dates}}, "
                f"not {tome_name!r}: the dates change when matches are added, and an "
                f"extended tome keeps its name. Use a name such as 'basic_{{channel}}'."
            )
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
        self._resume = bool(resume)
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
        self._header_id = None
        self._header_frame = None

    def run(self) -> BasicTomes:
        if self._if_complete == "extend":
            loader = self._loader(self._header_name)
            if loader.exists and loader.is_complete:
                return self._run_extend(loader)
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
        scribes = {
            c: self._scribe(self._staging_name(c), resume_info=self._resume_info(c))
            for c in channels
        }
        staged = self._resume_scribes(scribes) if self._resume else {}
        if header is not None or len(scribes) > 0:
            self._walk(paths, header, scribes, staged=staged)
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
                    f"{self._header_name}; pass behavior_if_complete='extend' "
                    f"(with a tome_name without {{dates}}) to add the keys it "
                    f"lacks, behavior_if_complete='overwrite' to rebuild it, or "
                    f"another header_tome_name"
                )
        self._header_keys = keys
        return keys

    def _header_paths(self):
        """The manifests to build the header from."""
        paths = self._candidate_paths()
        if len(paths) == 0:
            raise ValueError(f"No matches to build a header from in {self._ds_root}")
        return paths

    def _candidate_paths(self):
        """The manifests of the matches on offer: the given keys, or every one on disk."""
        if self._keys is None:
            return get_manifest_key_paths_from_glob(self._ds_root, self._ds_type)
        paths, dropped = dedupe(normalize_key(key) for key in self._keys)
        self._log_duplicates(dropped)
        return paths

    # The walk.

    def _walk(  # pylint: disable=too-many-locals
        self,
        paths,
        header,
        scribes,
        *,
        staged=None,
        published=None,
        known=frozenset(),
    ):
        """
        Visit each match once: manifest, header row and channels.

        A channel whose tome already holds a match (``staged``: the pages of
        an interrupted build that is resumed, ``published``: the finished
        tome that is extended) doesn't read that match's file again. A match
        in ``known`` is already in the header: its header row is left out.
        """
        # What an interrupted build left is started over, unless it is resumed.
        if not self._resume:
            self._remove_staging()
        staged = staged or {}
        channels = list(scribes)
        done = {
            c: staged.get(c, frozenset()) | (published or {}).get(c, frozenset())
            for c in channels
        }
        for scribe in scribes.values():
            scribe.start()
        if header is not None:
            header.start()
        progress = _Progress(self._log, len(paths))
        seen = set()
        duplicates = 0
        follow = _Follow(staged, published or {}, self._log)

        def read(path):
            return self._read_match(
                path,
                channels,
                with_header=header is not None,
                done=done,
                known=known,
            )

        for key, row, tables, present in _ordered_map(read, paths, self._threads):
            progress.step()
            if key in seen:
                duplicates += 1
                continue
            seen.add(key)
            if header is not None and key not in known:
                self._header_keys.append(key)
                header.add(key, row)
            follow.step(key, present)
            for channel, table in tables:
                scribes[channel].add(key, table)
        self._log_duplicates(duplicates)
        follow.check(self._staging_path())

    def _read_match(  # pylint: disable=too-many-locals
        self, path, channels, *, with_header, done=None, known=frozenset()
    ):
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
            if key not in known:
                if "header" not in by_name:
                    raise Exception("Channel header not found in replay.")
                table = read_channel(
                    self._channel_path(by_name["header"]), None, "header"
                )
                row = prepare_table(
                    table, [(HEADER_KEY_COLUMN, key), ("match_id", manifest["id"])]
                )
        present = [channel for channel in channels if channel in by_name]
        tables = []
        for channel in present:
            if done and key in done[channel]:
                # The tome has this match already: its file isn't read again.
                continue
            file_path = self._channel_path(by_name[channel])
            table = read_channel(file_path, self._columns[channel], channel)
            tables.append((channel, prepare_table(table, [(MATCH_KEY_COLUMN, key)])))
        return key, row, tables, present

    def _channel_path(self, entry) -> str:
        content_type = entry["contentType"]
        if content_type != _PARQUET:
            raise Exception(f"Unsupported content type {content_type}")
        return os.path.join(self._ds_root, os.path.normpath(entry["key"]))

    # Writing the tomes.

    def _scribe(self, name, *, is_header=False, resume_info=None):
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
            resume_info=resume_info,
            log=self._log,
        )

    def _resume_info(self, channel, *, base=None, first_page=0) -> dict:
        """
        What a channel's staged tome is built for, kept in its manifest: a
        staged tome built for anything else is not continued. ``base`` is the
        id of the finished tome it extends, and ``first_page`` the number of
        its first page.
        """
        return {
            "columns": self._columns[channel],
            "base": base,
            "firstPage": first_page,
        }

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

    # Resuming.

    def _resume_scribes(self, scribes) -> dict:
        """
        Continue the staged tome of each channel that has one this build can
        continue, and clear the staging folders of the others and of the
        header, which is written only when it is finished. Returns the keys
        each continued tome holds, by channel.
        """
        shutil.rmtree(
            os.path.join(self._staging_path(), _STAGED_HEADER), ignore_errors=True
        )
        staged = {}
        for channel, scribe in scribes.items():
            state = self._staged_state(channel, scribe.resume_info)
            if state is None:
                shutil.rmtree(
                    os.path.join(self._staging_path(), channel), ignore_errors=True
                )
                continue
            scribe.resume(state)
            staged[channel] = frozenset(state.keys)
            if state.pages:
                self._log.info(
                    "Resuming staged tome",
                    channel=channel,
                    pages=len(state.pages),
                    matches=len(state.keys),
                )
        return staged

    def _staged_state(self, channel, info):
        """The ledger of a channel's staged tome, or None when it can't be continued."""
        loader = self._loader(self._staging_name(channel))
        try:
            if not loader.exists:
                return None
            manifest = loader.manifest
        except (OSError, ValueError):
            self._log.warning(
                "Staged manifest unreadable: starting over", channel=channel
            )
            return None
        if manifest.get(RESUME_FIELD) != info or (
            manifest["pages"] and SIGNATURES_FIELD not in manifest
        ):
            self._log.warning(
                "Staged tome can't be continued (built for other columns or "
                "another tome, or by dsdk 4.1.1): starting over",
                channel=channel,
            )
            return None
        state = read_tome_state(self._tome_root, manifest)
        if state.dropped:
            self._log.warning(
                "Staged pages missing or unreadable: reading their matches again",
                channel=channel,
                pages=state.dropped,
            )
        return state

    def _publish(self, channel, scribe, name, staged_header=None):
        """
        Move a channel's staged tome to its name, with its header copy. An
        extension publishes before the header is committed, so its header copy
        is taken from the staged header (``staged_header``), not from the one
        on disk, which still lacks the new matches.
        """
        if scribe.key_count == 0:
            self._log.info("Channel in no matches: no tome", channel=channel)
            return
        scribe.finish()
        manifest = scribe.manifest
        final = dict(
            manifest, tome=name, key=make_key(["tome", self._ds_type, name, "tome"])
        )
        final.pop(RESUME_FIELD, None)
        # Mark the destination partial, with no pages, before any page file
        # there is replaced: an interrupted publish leaves a partial tome,
        # which the next build builds again, never a complete tome whose
        # pages are half old and half new.
        self._writer.write_manifest(dict(final, isComplete=False, pages=[]))
        pages = [self._move_page(page, name) for page in manifest["pages"]]
        if staged_header is None:
            self._copy_header(name, scribe.keys)
        else:
            self._write_header_copy(name, scribe.keys, staged_header)
        self._writer.write_manifest(dict(final, pages=pages))

    def _move_page(self, page, name) -> dict:
        """Move a staged page's files under the tome ``name``; the page entry as moved."""
        moved = dict(page)
        for subtype in ("dataframe", "keyset"):
            key = make_key(["tome", self._ds_type, name, page[subtype][subtype]])
            destination = os.path.join(self._tome_root, key)
            ensure_dir(destination)
            replace_file(get_page_path_fs(self._tome_root, subtype, page), destination)
            moved[subtype] = dict(page[subtype], key=key)
        return moved

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

    # Extending.

    def _run_extend(  # pylint: disable=too-many-locals
        self, header_loader
    ) -> BasicTomes:
        """
        Add the matches that the complete header lacks to its tomes.

        The new matches are read into new pages in the staging folder, with
        the header rows to append and the narrowing of the whole tome worked
        out; nothing in the finished tomes changes until all of that is done
        (a value that doesn't fit raises here). Then each finished tome is
        changed in turn: its new page files are moved beside the old ones
        (not listed yet), its header copy is written as a new page, and its
        manifest is replaced in one atomic step, which adds the pages. The
        channels come first and the header last: the header is the list of
        matches, so a build that stops in between finds the new matches
        missing from it and adds them again, and the channels that already
        hold them take nothing twice.
        """
        header_state = self._published_state(self._header_name, header_loader)
        old_keys, dropped = dedupe(header_state.keys)
        self._log_duplicates(dropped)
        known = frozenset(old_keys)
        self._header_keys = list(old_keys)
        self._header_id = header_state.manifest["id"]
        names = tome_names(self._tome_name, self._columns, None, self._header_name)
        self._tidy(self._header_name, header_state.manifest)
        extend, build = self._plan_channels(names)
        paths = self._candidate_paths()
        new_paths = [path for path in paths if normalize_key(path) not in known]
        self._log.info(
            "Extend Basic Tomes",
            matches_in_header=len(known),
            matches_on_offer=len(paths),
            matches_new=len(new_paths),
            tomes_extended=len(extend),
            tomes_built=len(build),
        )
        if not new_paths and not build:
            self._remove_staging()
            return self._result(names)

        header = self._scribe(self._staging_name(_STAGED_HEADER), is_header=True)
        self._seed_header(header, header_state)
        scribes, published = {}, {}
        for channel, state in extend.items():
            scribe = self._scribe(
                self._staging_name(channel),
                resume_info=self._resume_info(
                    channel,
                    base=state.manifest["id"],
                    first_page=state.next_page_number,
                ),
            )
            scribe.extend(state, tome_schemas(self._tome_root, state))
            scribes[channel] = scribe
            # The matches of the header are not added again, nor those a build
            # that stopped before it finished the header added to the tome.
            published[channel] = known.union(state.keys)
        for channel in build:
            scribes[channel] = self._scribe(
                self._staging_name(channel), resume_info=self._resume_info(channel)
            )
        staged = self._resume_scribes(scribes) if self._resume else {}
        # The tomes to build from scratch visit every match; the others the new ones.
        walk = (list(old_keys) if build else []) + new_paths
        self._walk(
            walk, header, scribes, staged=staged, published=published, known=known
        )

        added = len(self._header_keys) - len(old_keys)
        changed = {c: s for c, s in scribes.items() if c in extend and s.key_count > 0}
        built = {c: scribes[c] for c in build if scribes[c].key_count > 0}
        if added > 0:
            header.finish()
        for scribe in [*changed.values(), *built.values()]:
            # All of the checks that can fail come before the first change.
            scribe.finish()
        for channel, scribe in changed.items():
            self._commit_extension(
                channel, scribe, names[channel], extend[channel], header
            )
        # Channels first, the header last (see the docstring): a build that stops
        # after a channel is published finds the header still missing its matches.
        for channel in build:
            self._publish(channel, scribes[channel], names[channel], staged_header=header)
            if channel in built:
                self._tidy_published(names[channel])
        if added > 0:
            self._commit_header(header, header_state)
        self._remove_staging()
        self._log.info(
            "Extend Basic Tomes Done",
            matches_added=added,
            tomes_extended=len(changed),
            tomes_built=len(built),
            pages_rewritten=sum(len(s.rewrites) for s in changed.values()),
        )
        return self._result(names)

    def _result(self, names) -> BasicTomes:
        tomes = {}
        for channel, name in names.items():
            loader = self._loader(name)
            if loader.exists:
                tomes[channel] = loader
        header = self._loader(self._header_name)
        return BasicTomes(
            header=header, tomes=tomes, dates=header_dates(header.get_dataframe())
        )

    def _published_state(self, name, loader):
        """The ledger of a finished tome that is extended, which must be whole."""
        state = read_tome_state(self._tome_root, loader.manifest)
        if state.dropped:
            raise Exception(
                f"Tome {name} lists {state.dropped} page(s) whose files are missing "
                f"or unreadable, so it can't be extended. Build it again with "
                f"behavior_if_complete='overwrite'."
            )
        return state

    def _plan_channels(self, names):
        """
        Which channel tomes to extend (a finished one, by its ledger) and which
        to build over every match (none yet, or a partial one built again).
        """
        extend, build = {}, []
        for channel, name in names.items():
            warn_if_invalid_tome_name(name)
            loader = self._loader(name)
            if not loader.exists:
                build.append(channel)
            elif loader.is_complete:
                state = self._published_state(name, loader)
                self._tidy(name, state.manifest)
                extend[channel] = state
            elif self._if_partial == "fail":
                raise Exception(f"Tome already exists {name}")
            elif self._if_partial == "overwrite":
                build.append(channel)
        return extend, build

    def _seed_header(self, header, state):
        """
        Start the staged header with the rows of the finished one, so that
        the page written is one page of all the matches, joined as a build
        over all of them joins them.

        A page's rows count only for the keys its keyset lists: a build that
        stopped while it replaced the header (its rows first, then its keyset)
        leaves rows of matches that the keyset doesn't have yet, and they are
        read again from the matches.
        """
        header.extend(state, tome_schemas(self._tome_root, state), rewrite=False)
        for page, keys in zip(state.pages, state.page_keys):
            table = pq.read_table(get_page_path_fs(self._tome_root, "dataframe", page))
            if HEADER_KEY_COLUMN in table.column_names:
                # pyarrow.compute builds its functions on import: pylint can't see them.
                # pylint: disable=no-member
                listed = pc.is_in(
                    table[HEADER_KEY_COLUMN], value_set=pa.array(keys, pa.string())
                )
                if not pc.all(listed).as_py():
                    table = table.filter(listed)
            header.seed(keys, prepare_table(table, []))

    def _tome_dir(self, name) -> str:
        return os.path.join(self._tome_root, "tome", self._ds_type, name)

    def _read_manifest(self, key):
        """A manifest by its key, or None when it is missing or unreadable."""
        try:
            with open(
                os.path.join(self._tome_root, *key.split("/")), encoding="utf-8"
            ) as file:
                return json.load(file)
        except (OSError, ValueError):
            return None

    def _tidy(self, name, manifest):
        """Remove the page files that a build that stopped left beside a tome and its header copy."""
        removed = remove_orphans(self._tome_dir(name), manifest)
        copy = self._read_manifest(
            make_key(["tome", self._ds_type, name, "header", "tome"])
        )
        if copy is not None:
            removed += remove_orphans(
                os.path.join(self._tome_dir(name), "header"), copy
            )
        if removed:
            self._log.warning("Orphan page files removed", tome=name, files=removed)

    def _tidy_published(self, name):
        manifest = self._read_manifest(make_key(["tome", self._ds_type, name, "tome"]))
        if manifest is not None:
            self._tidy(name, manifest)

    def _commit_extension(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self, channel, scribe, name, state, header
    ):
        """
        Add a channel's new pages to its finished tome. The new page files
        come first and are not listed until the last step, which replaces the
        manifest in one atomic step, so a build that stops before it leaves
        the tome as it was, with files beside it that the next build removes.
        A page that the narrowing changed is replaced by a whole file, never
        half of one.
        """
        self._log.info(
            "Extending tome",
            channel=channel,
            matches=scribe.key_count,
            pages=len(scribe.manifest["pages"]),
            pages_rewritten=len(scribe.rewrites),
        )
        pages = [self._move_page(page, name) for page in scribe.manifest["pages"]]
        for page, staged_path in scribe.rewrites:
            replace_file(
                staged_path, get_page_path_fs(self._tome_root, "dataframe", page)
            )
        self._write_header_copy(name, [*state.keys, *scribe.keys], header)
        manifest = TomeManifest(
            tome_name=name,
            ds_type=self._ds_type,
            header_tome_name=self._header_name,
            log=self._log,
        )
        data = dict(state.manifest, pages=[*state.pages, *pages])
        data[SIGNATURES_FIELD] = scribe.manifest[SIGNATURES_FIELD]
        data["extendedAt"] = now_to_iso()
        manifest.set(data)
        manifest.finish()
        self._writer.write_manifest(manifest.get())
        remove_orphans(self._tome_dir(name), manifest.get())

    def _write_header_copy(self, name, keys, header):
        """
        The header copy of a tome that is extended, for all its ``keys``: one
        new page, taken from the staged header, and a manifest that replaces
        the old one (and keeps its id) in one atomic step.
        """
        if self._header_frame is None:
            # Read once for all the tomes: the staged header is the same for each.
            self._header_frame = read_pages_pandas(
                [
                    get_page_path_fs(self._tome_root, "dataframe", page)
                    for page in header.manifest["pages"]
                ]
            )
        frame = self._header_frame
        keys = frozenset(keys)
        frame = frame.loc[frame["key"].isin(keys) & ~frame["key"].duplicated()]
        frame = frame.reset_index(drop=True)
        copy_key = make_key(["tome", self._ds_type, name, "header", "tome"])
        old = self._read_manifest(copy_key)
        manifest = TomeManifest(
            tome_name=name,
            ds_type=self._ds_type,
            header_tome_name=self._header_name,
            is_header=True,
            src_id=self._header_id,
            is_copied_header=True,
            log=self._log,
        )
        manifest.start_page()
        page = manifest.end_page(next_page_number(old["pages"]) if old else 0)
        self._writer.write_page(page, frame, list(frame["key"]))
        manifest.finish()
        data = manifest.get()
        if old is not None:
            data.update(id=old["id"], createdAt=old["createdAt"])
        self._writer.write_manifest(data)
        remove_orphans(os.path.join(self._tome_dir(name), "header"), data)

    def _commit_header(self, header, state):
        """
        Replace the header's page by the staged one, which holds the new rows too.

        A header of one page, number 0, as this builder writes it, stays so:
        the files of page 0 (its rows, then its keyset, which is the list of
        matches) are replaced one by one, each whole, as tools that read
        ``header/keyset_00000`` expect. Any other header gets a new page and
        a manifest that lists only that one, in one atomic step.
        """
        staged = header.manifest
        if len(state.pages) == 1 and state.pages[0]["number"] == 0:
            pages = list(state.pages)
            # The rows first, the keyset (the list of matches) last. A stop between
            # the two leaves the old list, so the next extension adds the matches
            # again and rewrites the rows; it never leaves a listed match without rows.
            for subtype in ("dataframe", "keyset"):
                replace_file(
                    get_page_path_fs(self._tome_root, subtype, staged["pages"][0]),
                    get_page_path_fs(self._tome_root, subtype, pages[0]),
                )
        else:
            pages = [
                self._move_page(page, self._header_name) for page in staged["pages"]
            ]
        manifest = TomeManifest(
            tome_name=self._header_name, ds_type=self._ds_type, is_header=True
        )
        data = dict(state.manifest, pages=pages)
        data[SIGNATURES_FIELD] = staged[SIGNATURES_FIELD]
        data["extendedAt"] = now_to_iso()
        manifest.set(data)
        manifest.finish()
        self._writer.write_manifest(manifest.get())
        remove_orphans(self._tome_dir(self._header_name), manifest.get())

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
        resume_info=None,
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
        # What the staged tome is built for (see `_Build._resume_info`).
        self.resume_info = resume_info
        if resume_info is not None:
            self._manifest.get()[RESUME_FIELD] = resume_info
        # A finished tome that this one extends: its pages, which stay, and the
        # copies at other types that the narrowing makes of them, as (page,
        # staged file) pairs.
        self._base_pages = []
        self.rewrites = []
        self._finished = False

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

    def seed(self, keys, prepared):
        """
        Start the page with rows written before: the keys of the matches they
        came from, and the rows as (table, signature) or None.
        """
        self.keys.extend(keys)
        if prepared is not None:
            self._signatures.add(prepared[1])
        self._page.add_all(keys, prepared)

    def resume(self, state):
        """
        Continue a staged tome: its pages stay, the numbers go on after them
        and the keys and signatures they carry count as added. ``state`` is
        its `TomeState`, whose pages are the ones whose files are all there.
        """
        manifest = {
            name: value
            for name, value in state.manifest.items()
            if name not in ("completedAt", "timings")
        }
        manifest.update(pages=list(state.pages), isComplete=False)
        self._manifest.set(manifest)
        self._page_counter = max(self._page_counter, next_page_number(state.pages))
        self._page_keys = list(state.page_keys)
        self.keys = state.keys
        self._signatures.update(
            (schema, None, ())
            for schema in decode_schemas(manifest.get(SIGNATURES_FIELD, []))
        )

    def extend(self, state, schemas, *, rewrite=True):
        """
        Add pages to a finished tome (``state`` is its `TomeState`, ``schemas``
        what it was built from): new pages are numbered after its last, and,
        unless ``rewrite`` is False, its own pages are written again at the
        tome's types, if the narrowing changed them, when this tome finishes.
        Its pages and keys are not part of this tome's manifest.
        """
        self._signatures.update((schema, None, ()) for schema in schemas)
        self._page_counter = state.next_page_number
        if rewrite:
            self._base_pages = list(state.pages)

    def finish(self):
        if self._finished:
            return
        if len(self.keys) == 0:
            raise Exception("Empty Tome not supported")
        if len(self._page.keys) > 0:
            self._write()
        plan = plan_narrowing(signature[0] for signature in self._signatures)
        self._narrow_written_pages(plan)
        self._narrow_base_pages(plan)
        self._manifest.finish()
        self._manifest.get()[SIGNATURES_FIELD] = self._signature_texts()
        self._writer.write_manifest(self._manifest.get())
        self._finished = True

    def _signature_texts(self) -> list:
        """The schemas of the tome's matches, as the manifest keeps them."""
        return encode_schemas(signature[0] for signature in self._signatures)

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
        # The signatures of all the matches added so far are the ones of the
        # pages written: the page that was just written is the last they came in.
        self._manifest.get()[SIGNATURES_FIELD] = self._signature_texts()
        self._writer.write_manifest(self._manifest.get())
        self._page_keys.append(self._page.keys)
        self._page_counter += 1
        self._page = _Page(self._proxies, self._key_column)
        self._manifest.start_page()

    def _narrow_written_pages(self, plan):
        """
        Write again, at the tome's types, the pages that hold a column the
        tome narrows at another type: wide, because the page was written
        before a match with the narrow type or holds a value its matches'
        narrow type can't, or narrower, because a later match brought a wider
        narrow type. A value that doesn't fit the tome's type raises here.
        """
        for page, keys in zip(self._manifest.get()["pages"], self._page_keys):
            table, differ = self._narrowed(page, plan)
            if table is None:
                continue
            self._writer.write_page(page, table, keys)
            self.narrowed_pages += 1
            self._log.info(
                "Page narrowed",
                tome=self._manifest.get()["tome"],
                page_number=page["number"],
                columns=sorted(differ),
            )

    def _narrow_base_pages(self, plan):
        """
        The pages of the finished tome that this one extends, at the types the
        narrowing now gives: written aside, in the staging folder, as whole
        files that replace the old pages when the tome is changed. A value
        that doesn't fit the tome's type raises here, before anything changes.
        """
        folder = os.path.join(
            self._tome_root, *self._manifest.get()["key"].split("/")[:-1]
        )
        for page in self._base_pages:
            table, differ = self._narrowed(page, plan)
            if table is None:
                continue
            staged = os.path.join(folder, "rewrite_" + page["dataframe"]["dataframe"])
            self._writer.write_parquet(staged, table)
            self.rewrites.append((page, staged))
            self.narrowed_pages += 1
            self._log.info(
                "Page narrowed",
                tome=self._manifest.get()["tome"],
                page_number=page["number"],
                columns=sorted(differ),
            )

    def _narrowed(self, page, plan):
        """
        The page's table narrowed by the plan and the columns that changed,
        or (None, None) when the page already holds the plan's types.
        """
        if not plan:
            return None, None
        path = get_page_path_fs(self._tome_root, "dataframe", page)
        schema = pq.read_schema(path)
        differ = {
            name: target
            for name, target in plan.items()
            if name in schema.names
            and schema.field(name).type != target
            and not pa.types.is_null(schema.field(name).type)
        }
        if not differ:
            return None, None
        table = pq.read_table(path)
        return (
            narrow_table(table, differ, describe_rows(table, self._key_column)),
            differ,
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
        self.add_all([key], prepared)

    def add_all(self, keys, prepared):
        """Add the rows of one or more matches, as one table, with their keys."""
        self.keys.extend(keys)
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


class _Follow:
    """
    Checks, while a resumed build walks, that the matches come as they did
    when its staged pages were built.

    A staged tome holds the first matches of the walk that have its channel.
    If a match came in since (it sorts before a staged one) or the order of
    ``keys`` changed, the tome lists the staged matches first and the others
    after them, which is not the order of the header: that is logged. If a
    staged match is not found any more, the pages would hold a match that the
    header lacks, and resuming is refused.
    """

    def __init__(self, staged, published, log):
        self._staged = {channel: keys for channel, keys in staged.items() if keys}
        self._published = published
        self._log = log
        self._lacking = dict.fromkeys(self._staged, None)
        self._reached = {channel: set() for channel in self._staged}
        self._reordered = set()

    def step(self, key, present):
        for channel in present:
            keys = self._staged.get(channel)
            if keys is None:
                continue
            if key in keys:
                if self._lacking[channel] is not None:
                    self._reordered.add(channel)
                self._reached[channel].add(key)
            elif key not in self._published.get(channel, ()):
                if self._lacking[channel] is None:
                    self._lacking[channel] = key

    def check(self, staging_path):
        for channel, keys in self._staged.items():
            lost = keys - self._reached[channel]
            if lost:
                raise ValueError(
                    f"Can't resume: the staged pages of channel {channel} hold "
                    f"{len(lost)} match(es) that were not found this time, such as "
                    f"{sorted(lost)[0]!r}. Remove {staging_path} or build again "
                    f"without resume."
                )
        if self._reordered:
            self._log.warning(
                "Resumed tomes list the matches they staged first, then the "
                "others: not in the order of the header",
                channels=sorted(self._reordered),
            )


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
    """
    The page's tables, narrowed where its matches mix wide and narrow types,
    and the plan they were narrowed by.

    A value that doesn't fit int8 or int16 may still fit the wider narrow
    type a later match brings (int16 after int8), so the page keeps that
    column wide instead of raising. The tome's last step narrows it to the
    tome's type, and raises there if the value doesn't fit that either.
    Nothing wider than int32, float32 or bool can come, so those raise now.
    """
    plan = plan_narrowing(signature[0] for signature in signatures)
    while plan:
        try:
            narrowed = [
                narrow_table(item, plan, describe_rows(item, key_column))
                for item in items
            ]
        except NarrowingError as err:
            if plan.get(err.column) not in _MAY_WIDEN:
                raise
            plan = {name: t for name, t in plan.items() if name != err.column}
            continue
        return narrowed, plan
    return items, plan


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
    expected.update(
        missing_as_nullable(
            expected, narrow_int_columns(table.schema), flag_columns(plan)
        )
    )
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
    items, plan = narrow_items(items, signatures, key_column)
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
    nullable = missing_as_nullable(dict(frame.dtypes), narrow_ints, flag_columns(plan))
    for name, dtype in nullable.items():
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
