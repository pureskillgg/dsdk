"""
Resume and extend: what a build leaves on disk is its ledger.

A tome is a manifest and independent page files, and every page has a keyset
that names the matches whose rows it holds. Pages are never changed once the
manifest lists them, so a manifest and its keysets say exactly which matches a
tome holds, and which a build still has to read. `build_basic_tomes` uses that
in two ways:

``resume=True``
    Continue an interrupted build from its staging folder: the pages it
    finished stay, and only the matches and channels they lack are read.

``behavior_if_complete="extend"``
    Add the matches that appeared since a complete build to its tomes.

This module holds the parts that don't depend on the builder: reading a
tome's ledger from disk, the narrowing signatures the manifests carry, and
removing the files a crash leaves behind.

Two fields are added to the manifests the builder writes. Readers ignore
unknown manifest fields.

``signatures``
    The distinct Arrow schemas of the matches a tome holds, each serialised
    (Arrow IPC) and base64 encoded. The narrowing plan of a tome (see
    `narrowing`) depends on every schema that went into it, but a page keeps
    only the schema the plan gave it, so the pages alone can't say what a
    later match changes. A manifest without the field (written by dsdk 4.1.1
    or earlier) is read as the schemas of its pages, which is exact for a
    tome whose columns never mixed wide and narrow types and otherwise only
    means that a later match may widen fewer old pages.

``resume``
    Staging manifests only: what a staged tome was built for (the columns
    read, the tome it extends, the page number it starts at), so a staged
    tome built for something else is started over instead of continued.
"""

import base64
import os
import re
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.parquet as pq

from .constants import get_page_path_fs

SIGNATURES_FIELD = "signatures"
RESUME_FIELD = "resume"

_PAGE_FILE = re.compile(r"^(?:rewrite_)?(?:dataframe|keyset)_\d{5}$")  # rewrite_ is the staged copy of a page the narrowing changed
_INDEX_COLUMN = "__index_level_0__"
_KEYSET_COLUMN = "_"


def schema_to_text(schema) -> str:
    """A schema, without its metadata, as text that `schema_from_text` reads."""
    buffer = schema.remove_metadata().serialize()
    return base64.b64encode(buffer.to_pybytes()).decode("ascii")


def schema_from_text(text) -> pa.Schema:
    return pa.ipc.read_schema(pa.py_buffer(base64.b64decode(text)))


def encode_schemas(schemas) -> list:
    """The distinct schemas as sorted text, so a manifest doesn't change for nothing."""
    return sorted({schema_to_text(schema) for schema in schemas})


def decode_schemas(texts) -> list:
    return [schema_from_text(text) for text in texts]


def next_page_number(pages) -> int:
    """The number after the highest page number, 0 for no pages."""
    return max((page["number"] for page in pages), default=-1) + 1


def read_keyset(path) -> list:
    table = pq.read_table(path)
    name = _KEYSET_COLUMN if _KEYSET_COLUMN in table.column_names else None
    return table.column(name or 0).to_pylist()


@dataclass(frozen=True)
class TomeState:
    """
    A tome's ledger: the leading pages of its manifest whose files are all
    there, and the keys each holds.

    Attributes
    ----------
    manifest : dict
        The manifest as read.
    pages : list of dict
        The manifest's pages up to the first one whose dataframe or keyset
        file is missing or can't be read.
    page_keys : list of list of str
        The keyset of each page in ``pages``.
    dropped : int
        How many of the manifest's pages come after, and with, the first
        page that is missing or unreadable.
    """

    manifest: dict
    pages: list
    page_keys: list
    dropped: int

    @property
    def keys(self) -> list:
        return [key for keys in self.page_keys for key in keys]

    @property
    def next_page_number(self) -> int:
        return next_page_number(self.manifest["pages"])


def read_tome_state(tome_root, manifest) -> TomeState:
    """
    Read a tome's ledger. A page counts when its dataframe file has a
    readable parquet footer (a truncated file doesn't) and its keyset reads.
    The ledger stops at the first page that doesn't count: the matches of
    later pages are read again, so the pages stay in the order of the matches.
    """
    pages, page_keys = [], []
    for page in manifest["pages"]:
        try:
            pq.read_metadata(get_page_path_fs(tome_root, "dataframe", page))
            keys = read_keyset(get_page_path_fs(tome_root, "keyset", page))
        except (OSError, pa.ArrowException):
            break
        pages.append(page)
        page_keys.append(keys)
    return TomeState(
        manifest=manifest,
        pages=pages,
        page_keys=page_keys,
        dropped=len(manifest["pages"]) - len(pages),
    )


def tome_schemas(tome_root, state) -> list:
    """
    The schemas a tome was built from: its ``signatures`` field, or, for a
    manifest without one, the schemas of its pages.
    """
    texts = state.manifest.get(SIGNATURES_FIELD)
    if texts is not None:
        return decode_schemas(texts)
    schemas = set()
    for page in state.pages:
        schema = pq.read_schema(get_page_path_fs(tome_root, "dataframe", page))
        if _INDEX_COLUMN in schema.names:
            schema = pa.schema([f for f in schema if f.name != _INDEX_COLUMN])
        schemas.add(schema)
    return list(schemas)


def remove_orphans(folder, manifest) -> list:
    """
    Remove the page files in ``folder`` that ``manifest`` doesn't list, and
    temporary files of a manifest or page write that never finished. They are
    what a build that stopped between moving its pages and listing them
    leaves, or what a rewritten page replaced. Returns the names removed.
    """
    listed = {
        os.path.basename(page[subtype]["key"])
        for page in manifest["pages"]
        for subtype in ("dataframe", "keyset")
    }
    removed = []
    if not os.path.isdir(folder):
        return removed
    for entry in os.scandir(folder):
        if not entry.is_file() or entry.name in listed:
            continue
        if _PAGE_FILE.match(entry.name) or entry.name.endswith(".tmp"):
            try:
                os.remove(entry.path)
            except FileNotFoundError:
                continue
            removed.append(entry.name)
    return sorted(removed)
