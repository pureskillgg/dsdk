# pylint: disable=missing-docstring

import json
import os
import subprocess
import sys

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from . import writer_fs
from .constants import get_page_path_fs
from .incremental import remove_orphans
from .loader import TomeLoader
from .manifest import TomeManifest
from .reader_fs import TomeReaderFs
from .scribe import TomeScribe
from .writer_fs import TomeWriterFs

TOME_NAME = "writer_test.2022-01-01,2022-01-02"
DS_TYPE = "csds"


def make_frame(round_number):
    return pd.DataFrame(
        {
            "weapon": pd.Categorical(["ak47", "awp"], categories=["ak47", "awp"]),
            "round": pd.array([round_number, None], dtype="Int64"),
            "team": np.array([2, 3], dtype="int8"),
            "x": [1.5, -2.25],
            "at": pd.to_datetime(["2022-05-15T20:00:00Z", "2022-05-15T20:00:01Z"]),
            "name": ["alpha", "bravo"],
        }
    )


def write_tome(root_path, frames, **writer_kwargs):
    scribe = TomeScribe(
        manifest=TomeManifest(tome_name=TOME_NAME, ds_type=DS_TYPE),
        writer=TomeWriterFs(root_path=root_path, **writer_kwargs),
        max_page_row_count=3,
        limit_check_frequency=1,
    )
    scribe.start()
    for i, frame in enumerate(frames):
        scribe.concat(frame, f"match-{i}")
    scribe.finish()
    reader = TomeReaderFs(
        root_path=root_path,
        manifest_key="/".join(["tome", DS_TYPE, TOME_NAME, "tome"]),
        has_header=False,
    )
    return TomeLoader(reader=reader, has_header=False)


def page_codecs(root_path, loader):
    """The codec of every column chunk in every page file, dataframe and keyset."""
    codecs = set()
    for page in loader.manifest["pages"]:
        for subtype in ["dataframe", "keyset"]:
            metadata = pq.ParquetFile(
                get_page_path_fs(root_path, subtype, page)
            ).metadata
            for i in range(metadata.num_row_groups):
                row_group = metadata.row_group(i)
                for j in range(row_group.num_columns):
                    codecs.add(row_group.column(j).compression)
    return codecs


def test_pages_default_to_zstd(tmp_path):
    root_path = str(tmp_path)
    loader = write_tome(root_path, [make_frame(1)])

    page = loader.manifest["pages"][0]
    path = get_page_path_fs(root_path, "dataframe", page)
    assert pq.ParquetFile(path).metadata.row_group(0).column(0).compression == "ZSTD"
    assert page_codecs(root_path, loader) == {"ZSTD"}


@pytest.mark.parametrize(
    "compression,codec",
    [("zstd", "ZSTD"), ("gzip", "GZIP"), ("snappy", "SNAPPY"), (None, "UNCOMPRESSED")],
)
def test_pages_round_trip_with_any_codec(tmp_path, compression, codec):
    root_path = str(tmp_path)
    frames = [make_frame(i) for i in range(3)]

    loader = write_tome(root_path, frames, compression=compression)

    assert len(loader.manifest["pages"]) == 2
    assert page_codecs(root_path, loader) == {codec}
    assert loader.get_keyset() == ["match-0", "match-1", "match-2"]
    pd.testing.assert_frame_equal(
        loader.get_dataframe().reset_index(drop=True),
        pd.concat(frames, ignore_index=True),
    )


@pytest.mark.parametrize("compression,codec", [("zstd", "ZSTD"), ("gzip", "GZIP")])
def test_a_pyarrow_table_page_is_written_as_it_is(tmp_path, compression, codec):
    root_path = str(tmp_path)
    frame = make_frame(1)
    table = pa.Table.from_pandas(frame, preserve_index=False)
    manifest = TomeManifest(tome_name=TOME_NAME, ds_type=DS_TYPE)
    writer = TomeWriterFs(root_path=root_path, compression=compression)
    manifest.start_page()
    page = manifest.end_page(0)

    writer.write_page(page, table, ["match-0"])

    path = get_page_path_fs(root_path, "dataframe", page)
    assert pq.ParquetFile(path).metadata.row_group(0).column(0).compression == codec
    assert pq.read_schema(path).pandas_metadata == table.schema.pandas_metadata
    pd.testing.assert_frame_equal(pd.read_parquet(path), frame)
    keyset = pd.read_parquet(get_page_path_fs(root_path, "keyset", page))
    assert list(keyset["_"]) == ["match-0"]


def test_unknown_codec_fails_before_any_page_is_written(tmp_path):
    with pytest.raises(ValueError, match="Unsupported parquet compression.*'zstdd'"):
        TomeWriterFs(root_path=str(tmp_path), compression="zstdd")


# Writes are all or nothing.

MANIFEST_KEY = "tome/csds/t/tome"
OLD_MANIFEST = {"key": MANIFEST_KEY, "pages": [], "isComplete": False}
NEW_MANIFEST = {"key": MANIFEST_KEY, "pages": ["p"] * 500, "isComplete": True}

# A process that dies half way through writing a manifest, without a chance to
# clean up, as a kill or a power cut does.
DIES_WRITING_A_MANIFEST = """
import os, sys
import rapidjson
from pureskillgg_dsdk.tome.writer_fs import TomeWriterFs

def dump(data, file):
    text = rapidjson.dumps(data)
    file.write(text[: len(text) // 2])
    file.flush()
    os._exit(7)

rapidjson.dump = dump
TomeWriterFs(root_path=sys.argv[1]).write_manifest(
    {"key": sys.argv[2], "pages": ["p"] * 500, "isComplete": True}
)
"""


def read_manifest(root_path):
    with open(os.path.join(root_path, *MANIFEST_KEY.split("/")), encoding="utf-8") as f:
        return json.load(f)


def files_beside(root_path):
    return sorted(os.listdir(os.path.dirname(os.path.join(root_path, MANIFEST_KEY))))


def test_a_manifest_is_replaced_whole(tmp_path):
    writer = TomeWriterFs(root_path=str(tmp_path))
    writer.write_manifest(OLD_MANIFEST)

    writer.write_manifest(NEW_MANIFEST)

    assert read_manifest(str(tmp_path)) == NEW_MANIFEST
    assert files_beside(str(tmp_path)) == ["tome"]


def test_a_process_killed_while_writing_a_manifest_leaves_the_old_one(tmp_path):
    root_path = str(tmp_path)
    TomeWriterFs(root_path=root_path).write_manifest(OLD_MANIFEST)

    crashed = subprocess.run(
        [sys.executable, "-c", DIES_WRITING_A_MANIFEST, root_path, MANIFEST_KEY],
        check=False,
    )

    assert crashed.returncode == 7
    # The manifest is the old one, complete; the half-written one is beside it.
    assert read_manifest(root_path) == OLD_MANIFEST
    stray = [name for name in files_beside(root_path) if name.endswith(".tmp")]
    assert len(stray) == 1
    # The next write goes through, and the next build removes the stray file.
    writer = TomeWriterFs(root_path=root_path)
    writer.write_manifest(NEW_MANIFEST)
    assert read_manifest(root_path) == NEW_MANIFEST
    folder = os.path.dirname(os.path.join(root_path, MANIFEST_KEY))
    assert remove_orphans(folder, {"pages": []}) == stray
    assert files_beside(root_path) == ["tome"]


def test_a_failed_manifest_write_leaves_the_old_one_and_no_stray_file(
    tmp_path, monkeypatch
):
    root_path = str(tmp_path)
    writer = TomeWriterFs(root_path=root_path)
    writer.write_manifest(OLD_MANIFEST)

    def fail(*_):
        raise OSError("disk full")

    monkeypatch.setattr(os, "replace", fail)
    with pytest.raises(OSError, match="disk full"):
        writer.write_manifest(NEW_MANIFEST)
    monkeypatch.undo()

    assert read_manifest(root_path) == OLD_MANIFEST
    assert files_beside(root_path) == ["tome"]


def test_a_file_in_use_is_replaced_once_it_is_free(tmp_path, monkeypatch):
    # Windows refuses os.replace while another process has the file open.
    root_path = str(tmp_path)
    writer = TomeWriterFs(root_path=root_path)
    writer.write_manifest(OLD_MANIFEST)
    real = os.replace
    refusals = {"left": 2}

    def busy(source, destination):
        if refusals["left"] > 0:
            refusals["left"] -= 1
            raise PermissionError("in use")
        real(source, destination)

    monkeypatch.setattr(os, "replace", busy)
    monkeypatch.setattr(writer_fs.time, "sleep", lambda _: None)
    writer.write_manifest(NEW_MANIFEST)
    monkeypatch.undo()

    assert refusals["left"] == 0
    assert read_manifest(root_path) == NEW_MANIFEST


def test_a_failed_page_write_leaves_the_old_page(tmp_path, monkeypatch):
    root_path = str(tmp_path)
    writer = TomeWriterFs(root_path=root_path)
    manifest = TomeManifest(tome_name=TOME_NAME, ds_type=DS_TYPE)
    manifest.start_page()
    page = manifest.end_page(0)
    writer.write_page(page, make_frame(1), ["match-0"])
    path = get_page_path_fs(root_path, "dataframe", page)

    def fail(_table, file, **_):
        file.write(b"PAR1 half a page")
        raise OSError("disk full")

    monkeypatch.setattr(pq, "write_table", fail)
    with pytest.raises(OSError, match="disk full"):
        writer.write_page(page, pa.Table.from_pandas(make_frame(2)), ["match-1"])
    monkeypatch.undo()

    pd.testing.assert_frame_equal(pd.read_parquet(path), make_frame(1))
    assert sorted(os.listdir(os.path.dirname(path))) == [
        "dataframe_00000",
        "keyset_00000",
    ]
