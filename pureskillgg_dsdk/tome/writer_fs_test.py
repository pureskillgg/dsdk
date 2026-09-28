# pylint: disable=missing-docstring

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

from .constants import get_page_path_fs
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


def test_unknown_codec_fails_before_any_page_is_written(tmp_path):
    with pytest.raises(ValueError, match="Unsupported parquet compression.*'zstdd'"):
        TomeWriterFs(root_path=str(tmp_path), compression="zstdd")
