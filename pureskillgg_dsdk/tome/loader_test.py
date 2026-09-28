# pylint: disable=missing-docstring

import sys
import warnings

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from . import page_frames
from .constants import get_page_path_fs
from .loader import TomeLoader
from .manifest import TomeManifest
from .reader_fs import TomeReaderFs
from .scribe import TomeScribe
from .writer_fs import TomeWriterFs

TOME_NAME = "loader_test.2022-01-01,2022-01-02"
DS_TYPE = "csds"


def zstd(df, path):
    """A page as dsdk 3.3 writes it: zstd, index in the metadata only."""
    df.to_parquet(path, compression="zstd")


def gzip_with_index(df, path):
    """A page as dsdk 3.2.1 and earlier wrote it: gzip, index as a column."""
    df.set_axis(pd.Index(range(10, 10 + len(df)))).to_parquet(
        path, compression="gzip", index=True
    )


def arrow_only(df, path):
    """A page with no pandas metadata, as polars writes it."""
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table.replace_schema_metadata(None), path)


def no_statistics(df, path):
    """A page whose footer has no min or max for its columns."""
    df.to_parquet(path, compression="zstd", write_statistics=False)


def write_tome(root_path, pages):
    """A tome whose page i is pages[i]: a (writer, frame) pair."""
    manifest = TomeManifest(tome_name=TOME_NAME, ds_type=DS_TYPE)
    writer = TomeWriterFs(root_path=root_path)
    manifest.start_page()
    for number, (write, df) in enumerate(pages):
        page = manifest.end_page(number)
        writer.write_page(page, pd.DataFrame(), [f"match-{number}"])
        write(df, get_page_path_fs(root_path, "dataframe", page))
        manifest.start_page()
    manifest.finish()
    writer.write_manifest(manifest.get())
    return create_loader(root_path)


def create_loader(root_path):
    reader = TomeReaderFs(
        root_path=root_path,
        manifest_key="/".join(["tome", DS_TYPE, TOME_NAME, "tome"]),
        has_header=False,
    )
    return TomeLoader(reader=reader, has_header=False)


def page_paths(root_path, loader):
    return [
        get_page_path_fs(root_path, "dataframe", page)
        for page in loader.manifest["pages"]
    ]


def concat_pages(root_path, loader, columns=None):
    """What get_dataframe returned before dsdk 3.3, with a clean index."""
    with warnings.catch_warnings():
        # pandas 2 warns that it ignores all-NA columns when picking dtypes.
        warnings.simplefilter("ignore", FutureWarning)
        frames = [pd.read_parquet(path) for path in page_paths(root_path, loader)]
        df = pd.concat(frames, ignore_index=True)
    return df if columns is None else df[columns]


def read(loader, **kwargs):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        return loader.get_dataframe(**kwargs)


def game_frame(offset):
    """A page like a csds channel: ids, nullable ints, strings, time, flags."""
    return pd.DataFrame(
        {
            "round": pd.array([offset + 1, None, offset + 2], dtype="Int64"),
            "tick": np.array([10, 20, 30], dtype="int32") + offset,
            "player_id": [offset + 1, offset + 2, offset + 3],
            "weapon": pd.Categorical(
                ["ak47", "awp", "ak47"], categories=["ak47", "awp"]
            ),
            "x": [1.5, np.nan, -2.25],
            "name": ["alpha", None, "charlie"],
            "is_alive": [True, False, True],
            "at": pd.to_datetime(
                ["2022-05-15T20:00:00Z", "2022-05-15T20:00:01Z", "2022-05-15T20:00:02Z"]
            ),
        }
    )


def frame(**columns):
    return pd.DataFrame(columns)


def ints(*values, dtype="Int64"):
    return pd.array(list(values), dtype=dtype)


# Each case is a tome: a list of (writer, frame) pages.
CASES = {
    "gzip pages with an index column": [
        (gzip_with_index, game_frame(0)),
        (gzip_with_index, game_frame(100)),
    ],
    "zstd pages": [(zstd, game_frame(0)), (zstd, game_frame(100))],
    "gzip pages continued with zstd": [
        (gzip_with_index, game_frame(0)),
        (zstd, game_frame(100)),
        (zstd, game_frame(200)),
    ],
    "one page": [(zstd, game_frame(0))],
    "pages without pandas metadata": [
        (arrow_only, frame(a=[1, 2], b=["x", "y"])),
        (arrow_only, frame(a=[3, 4], b=["z", None])),
    ],
    "pages with and without pandas metadata": [
        (arrow_only, frame(a=[1, 2], b=["x", "y"])),
        (zstd, frame(a=ints(3, None), b=["z", None])),
    ],
    "int64 drifts to double": [
        (zstd, frame(player_id_fixed=[1, 2])),
        (zstd, frame(player_id_fixed=[1.0, np.nan])),
        (zstd, frame(player_id_fixed=[3, 4])),
    ],
    "double drifts to int64 of 2**53": [
        (zstd, frame(player_id_fixed=[1.5, np.nan])),
        (zstd, frame(player_id_fixed=[2**53, -(2**53)])),
    ],
    "int64 past 2**53 drifts to double": [
        (zstd, frame(player_id_fixed=[2**53 + 1, 76561198000000001, -(2**62) - 3])),
        (zstd, frame(player_id_fixed=[1.5, np.nan])),
    ],
    "int64 without statistics drifts to double": [
        (no_statistics, frame(player_id_fixed=[1, 2])),
        (zstd, frame(player_id_fixed=[1.5, np.nan])),
    ],
    "metadata int64 then Int64": [
        (zstd, frame(player_id=[1, 2])),
        (zstd, frame(player_id=ints(3, None))),
    ],
    "metadata Int64 then int64": [
        (zstd, frame(player_id=ints(3, None))),
        (zstd, frame(player_id=[1, 2])),
    ],
    "metadata Int64 without nulls then int64": [
        (zstd, frame(player_id=ints(3, 4))),
        (zstd, frame(player_id=[1, 2])),
    ],
    "Int64 first on a later page, which the first page lacks": [
        (zstd, frame(a=[1, 2])),
        (zstd, frame(a=[3, 4], player_id=ints(5, None))),
    ],
    "Int64 then double": [
        (zstd, frame(a=ints(3, None))),
        (zstd, frame(a=[1.5, np.nan])),
    ],
    "double then Int64": [
        (zstd, frame(a=[1.5, np.nan])),
        (zstd, frame(a=ints(3, None))),
    ],
    "int32 then int64": [
        (zstd, frame(a=np.array([1, 2], dtype="int32"))),
        (zstd, frame(a=[3, 4])),
    ],
    "a column added on a later page": [
        (zstd, frame(a=[1, 2])),
        (zstd, frame(a=[3, 4], b=[1.5, 2.5], c=["x", "y"])),
    ],
    "columns dropped on a later page": [
        (zstd, frame(a=[1, 2], b=[1, 2], c=[True, False], d=ints(1, None))),
        (zstd, frame(a=[3, 4])),
    ],
    "columns in another order": [
        (zstd, frame(a=[1], b=["x"])),
        (zstd, frame(b=["y"], a=[2])),
    ],
    "null-typed column, then doubles": [
        (zstd, frame(a=[None, None], b=[1, 2])),
        (zstd, frame(a=[1.5, 2.5], b=[3, 4])),
    ],
    "null-typed column, then strings": [
        (zstd, frame(a=[None, None])),
        (zstd, frame(a=["x", "y"])),
    ],
    "strings, then a null-typed column": [
        (zstd, frame(a=["x", "y"])),
        (zstd, frame(a=[None, None])),
    ],
    "null-typed column, then Int64": [
        (zstd, frame(a=[None, None])),
        (zstd, frame(a=ints(1, None))),
    ],
    "null-typed column on every page": [
        (zstd, frame(a=[None, None], b=[1, 2])),
        (zstd, frame(a=[None], b=[3])),
    ],
    "categories differ": [
        (zstd, frame(a=pd.Categorical(["x", "y"]))),
        (zstd, frame(a=pd.Categorical(["z"]))),
    ],
    "category then strings": [
        (zstd, frame(a=pd.Categorical(["x", "y"]))),
        (zstd, frame(a=["z", "w"])),
    ],
    "bool then int": [
        (zstd, frame(a=[True, False])),
        (zstd, frame(a=[1, 2])),
    ],
    "an empty last page": [
        (zstd, game_frame(0)),
        (zstd, pd.DataFrame()),
    ],
    "an empty first page": [
        (zstd, pd.DataFrame()),
        (zstd, game_frame(0)),
    ],
    "only an empty page": [(zstd, pd.DataFrame())],
}

# Selections of columns, per case, for columns=.
SELECTIONS = {
    "gzip pages with an index column": ["name", "round", "x"],
    "gzip pages continued with zstd": ["weapon", "player_id"],
    "int64 drifts to double": ["player_id_fixed"],
    "Int64 first on a later page, which the first page lacks": ["player_id", "a"],
    "a column added on a later page": ["c", "a"],
    "columns dropped on a later page": ["d", "a", "c"],
    "null-typed column, then doubles": ["b", "a"],
}


@pytest.mark.parametrize("pages", CASES.values(), ids=CASES.keys())
def test_get_dataframe_equals_the_pages_concatenated(tmp_path, pages):
    root_path = str(tmp_path)
    loader = write_tome(root_path, pages)

    df = read(loader)

    expected = concat_pages(root_path, loader)
    assert isinstance(df.index, pd.RangeIndex)
    pd.testing.assert_index_equal(df.index, pd.RangeIndex(len(expected)))
    pd.testing.assert_frame_equal(df, expected)


@pytest.mark.parametrize("case", SELECTIONS.keys())
def test_get_dataframe_reads_selected_columns(tmp_path, case):
    root_path = str(tmp_path)
    loader = write_tome(root_path, CASES[case])
    columns = SELECTIONS[case]

    df = read(loader, columns=columns)

    pd.testing.assert_frame_equal(df, concat_pages(root_path, loader, columns))


def test_old_pages_lose_their_index(tmp_path):
    root_path = str(tmp_path)
    loader = write_tome(root_path, CASES["gzip pages with an index column"])

    before = pd.concat(
        [pd.read_parquet(path) for path in page_paths(root_path, loader)]
    )
    df = loader.get_dataframe()

    assert list(before.index) == [10, 11, 12, 10, 11, 12]
    assert "__index_level_0__" not in df.columns
    pd.testing.assert_index_equal(df.index, pd.RangeIndex(6))
    pd.testing.assert_frame_equal(df, before.reset_index(drop=True))


def test_the_drift_real_tomes_have_takes_one_dataset_read(tmp_path, monkeypatch):
    root_path = str(tmp_path)
    # Real tomes mix int64 and Int64 id columns, and int64 and double
    # player_id_fixed. No category column: categories live in the data, so a
    # category column of several pages is read page by page.
    pages = [
        (
            gzip_with_index,
            game_frame(0).drop(columns="weapon").assign(player_id_fixed=[1, 2, 3]),
        ),
        (
            zstd,
            game_frame(100)
            .drop(columns="weapon")
            .assign(player_id=ints(1, None, 3), player_id_fixed=[1.0, np.nan, 3.0]),
        ),
        (zstd, pd.DataFrame()),
    ]
    loader = write_tome(root_path, pages)
    expected = concat_pages(root_path, loader)

    def fail(*_args, **_kwargs):
        raise AssertionError("read page by page")

    monkeypatch.setattr(page_frames.pd, "read_parquet", fail)

    pd.testing.assert_frame_equal(loader.get_dataframe(), expected)
    assert expected["player_id"].dtype == "Int64"
    assert expected["player_id_fixed"].dtype == "float64"


def record_page_reads(monkeypatch):
    """The columns of each page read with pd.read_parquet, in order."""
    read_columns = []
    read_parquet = pd.read_parquet

    def record(path, columns=None, **kwargs):
        read_columns.append(columns)
        return read_parquet(path, columns=columns, **kwargs)

    monkeypatch.setattr(page_frames.pd, "read_parquet", record)
    return read_columns


@pytest.mark.parametrize(
    "case",
    [
        "int64 past 2**53 drifts to double",
        "int64 without statistics drifts to double",
    ],
)
def test_ints_a_double_may_not_hold_are_read_page_by_page(tmp_path, monkeypatch, case):
    root_path = str(tmp_path)
    loader = write_tome(root_path, CASES[case])
    read_columns = record_page_reads(monkeypatch)

    df = loader.get_dataframe()

    assert read_columns == [["player_id_fixed"], ["player_id_fixed"]]
    pd.testing.assert_frame_equal(df, concat_pages(root_path, loader))


def test_only_drifting_columns_are_read_page_by_page(tmp_path, monkeypatch):
    root_path = str(tmp_path)
    pages = [
        (zstd, frame(a=[1, 2], round=ints(1, None), b=["x", "y"])),
        (zstd, frame(a=[3, 4], round=[1.5, np.nan], b=["z", "w"])),
    ]
    loader = write_tome(root_path, pages)
    read_columns = record_page_reads(monkeypatch)

    df = loader.get_dataframe()

    assert read_columns == [["round"], ["round"]]
    assert list(df.columns) == ["a", "round", "b"]
    assert df["round"].dtype == "Float64"
    pd.testing.assert_frame_equal(df, concat_pages(root_path, loader))


def test_attrs_are_kept_when_every_page_has_the_same(tmp_path):
    root_path = str(tmp_path)
    same = [frame(a=[1]), frame(a=[2])]
    for df in same:
        df.attrs = {"source": "csds"}
    loader = write_tome(root_path, [(zstd, df) for df in same])

    assert loader.get_dataframe().attrs == {"source": "csds"}
    assert concat_pages(root_path, loader).attrs == {"source": "csds"}


def test_attrs_in_another_key_order_are_the_same(tmp_path):
    root_path = str(tmp_path)
    pages = [frame(a=[1]), frame(a=[2])]
    pages[0].attrs = {"source": "csds", "version": 1}
    pages[1].attrs = {"version": 1, "source": "csds"}
    loader = write_tome(root_path, [(zstd, df) for df in pages])

    expected = concat_pages(root_path, loader).attrs
    assert expected == {"source": "csds", "version": 1}
    assert loader.get_dataframe().attrs == expected


def test_an_empty_page_keeps_the_attrs(tmp_path):
    # pd.concat leaves a page with no rows and no columns out, attrs included.
    root_path = str(tmp_path)
    data = frame(a=[1, 2])
    data.attrs = {"source": "csds"}
    loader = write_tome(root_path, [(zstd, data), (zstd, pd.DataFrame())])

    expected = concat_pages(root_path, loader).attrs
    assert expected == {"source": "csds"}
    assert loader.get_dataframe().attrs == expected


def test_attrs_are_dropped_when_pages_differ(tmp_path):
    root_path = str(tmp_path)
    differ = [frame(a=[1]), frame(a=[2])]
    differ[0].attrs = {"source": "csds"}
    loader = write_tome(root_path, [(zstd, df) for df in differ])

    assert loader.get_dataframe().attrs == {}
    assert concat_pages(root_path, loader).attrs == {}


def test_unknown_columns_fail(tmp_path):
    loader = write_tome(str(tmp_path), CASES["zstd pages"])

    with pytest.raises(KeyError, match="nope"):
        loader.get_dataframe(columns=["round", "nope"])
    with pytest.raises(KeyError, match="__index_level_0__"):
        loader.get_dataframe(columns=["__index_level_0__"])
    with pytest.raises(TypeError, match="list of column names"):
        loader.get_dataframe(columns="round")


def test_unknown_library_fails(tmp_path):
    loader = write_tome(str(tmp_path), CASES["zstd pages"])

    with pytest.raises(ValueError, match="library must be one of"):
        loader.get_dataframe(library="spark")


def test_a_tome_without_pages_fails(tmp_path):
    root_path = str(tmp_path)
    scribe = TomeScribe(
        manifest=TomeManifest(tome_name=TOME_NAME, ds_type=DS_TYPE),
        writer=TomeWriterFs(root_path=root_path),
    )
    scribe.start()
    loader = create_loader(root_path)

    assert loader.manifest["pages"] == []
    with pytest.raises(ValueError, match="no pages"):
        loader.get_dataframe()


def test_a_relative_root_path_reads(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    loader = write_tome("tomes", CASES["zstd pages"])

    pd.testing.assert_frame_equal(loader.get_dataframe(), concat_pages("tomes", loader))


def test_strings_map_like_read_parquet_before_pyarrow_19(tmp_path, monkeypatch):
    root_path = str(tmp_path)
    loader = write_tome(root_path, CASES["zstd pages"])
    monkeypatch.setattr(page_frames, "_PYARROW_MAJOR", 18)

    pd.testing.assert_frame_equal(
        loader.get_dataframe(), concat_pages(root_path, loader)
    )


def test_polars_missing_gives_a_clear_error(tmp_path, monkeypatch):
    loader = write_tome(str(tmp_path), CASES["zstd pages"])
    monkeypatch.setitem(sys.modules, "polars", None)

    with pytest.raises(ImportError, match=r"pureskillgg-dsdk\[polars\]"):
        loader.get_dataframe(library="polars")
    with pytest.raises(ImportError, match=r"pureskillgg-dsdk\[polars\]"):
        loader.scan()


# Cases polars reads with other values than pandas: polars has no mixed
# object columns, so a bool column joined with ints becomes ints.
POLARS_CASES = {
    name: pages for name, pages in CASES.items() if name not in {"bool then int"}
}


def to_python(values):
    """Values with every missing marker (None, NaN, NaT, pd.NA) as None."""
    return [None if pd.isna(value) else value for value in values]


def assert_same_values(polars_frame, expected):
    assert polars_frame.columns == list(expected.columns)
    assert polars_frame.height == len(expected)
    for name in expected.columns:
        assert to_python(polars_frame[name].to_list()) == to_python(
            expected[name].tolist()
        ), name


@pytest.mark.parametrize("pages", POLARS_CASES.values(), ids=POLARS_CASES.keys())
def test_polars_values_equal_pandas(tmp_path, pages):
    pl = pytest.importorskip("polars")
    root_path = str(tmp_path)
    loader = write_tome(root_path, pages)

    df = loader.get_dataframe(library="polars")

    assert isinstance(df, pl.DataFrame)
    assert_same_values(df, concat_pages(root_path, loader))


@pytest.mark.parametrize("case", SELECTIONS.keys())
def test_polars_reads_selected_columns(tmp_path, case):
    pytest.importorskip("polars")
    root_path = str(tmp_path)
    loader = write_tome(root_path, CASES[case])
    columns = SELECTIONS[case]

    df = loader.get_dataframe(columns=columns, library="polars")

    assert_same_values(df, concat_pages(root_path, loader, columns))


def test_scan_is_lazy_and_filters(tmp_path):
    pl = pytest.importorskip("polars")
    root_path = str(tmp_path)
    loader = write_tome(root_path, CASES["gzip pages continued with zstd"])

    lazy = loader.scan()
    query = lazy.filter(pl.col("player_id") > 100).select(["player_id", "name"])

    assert isinstance(lazy, pl.LazyFrame)
    assert "__index_level_0__" not in lazy.collect_schema().names()
    expected = concat_pages(root_path, loader)
    expected = expected.loc[expected["player_id"] > 100, ["player_id", "name"]]
    assert_same_values(query.collect(), expected)
