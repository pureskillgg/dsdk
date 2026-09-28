# pylint: disable=missing-docstring

import numpy as np
import pandas as pd

from .loader import TomeLoader
from .manifest import TomeManifest
from .reader_fs import TomeReaderFs
from .scribe import TomeScribe
from .writer_fs import TomeWriterFs

TOME_NAME = "scribe_test.2022-01-01,2022-01-02"
DS_TYPE = "csds"
WEAPONS = ["ak47", "awp", "m4a1"]


def create_scribe(root_path, **kwargs):
    return TomeScribe(
        manifest=TomeManifest(tome_name=TOME_NAME, ds_type=DS_TYPE),
        writer=TomeWriterFs(root_path=root_path),
        **kwargs,
    )


def create_loader(root_path):
    reader = TomeReaderFs(
        root_path=root_path,
        manifest_key="/".join(["tome", DS_TYPE, TOME_NAME, "tome"]),
        has_header=False,
    )
    return TomeLoader(reader=reader, has_header=False)


def make_frame(round_number):
    return pd.DataFrame(
        {
            "weapon": pd.Categorical(["ak47", "awp"], categories=WEAPONS),
            "round": pd.array([round_number, None], dtype="Int64"),
            "team": np.array([2, 3], dtype="int8"),
            "is_alive": [True, False],
            "at": pd.to_datetime(["2022-05-15T20:00:00Z", "2022-05-15T20:00:01Z"]),
            "name": ["alpha", "bravo"],
        }
    )


def test_page_round_trip_keeps_dtypes_and_values(tmp_path):
    root_path = str(tmp_path)
    frames = [make_frame(1), make_frame(2)]
    scribe = create_scribe(root_path)

    scribe.start()
    scribe.concat(frames[0], "match-a")
    scribe.concat(frames[1], "match-b")
    scribe.finish()

    loader = create_loader(root_path)
    expected = pd.concat(frames, ignore_index=True)
    assert len(loader.manifest["pages"]) == 1
    assert loader.get_keyset() == ["match-a", "match-b"]
    pd.testing.assert_frame_equal(loader.get_dataframe(), expected)


def test_page_is_the_union_of_columns(tmp_path):
    scribe = create_scribe(str(tmp_path))

    scribe.concat(pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}), "match-a")
    scribe.concat(pd.DataFrame({"b": ["z"], "c": [1.5]}), "match-b")

    expected = pd.DataFrame(
        {"a": [1.0, 2.0, np.nan], "b": ["x", "y", "z"], "c": [np.nan, np.nan, 1.5]}
    )
    pd.testing.assert_frame_equal(scribe.dataframe, expected)


def test_concat_without_rows_still_records_keys(tmp_path):
    scribe = create_scribe(str(tmp_path))

    scribe.concat(None, "match-a")
    scribe.concat(pd.DataFrame({"a": pd.Series([], dtype="object")}), "match-b")
    scribe.concat(pd.DataFrame({"b": [1, 2]}), "match-c")

    assert scribe.keyset == ["match-a", "match-b", "match-c"]
    assert scribe.page_row_count == 2
    pd.testing.assert_frame_equal(scribe.dataframe, pd.DataFrame({"b": [1, 2]}))


def test_page_does_not_change_when_the_source_frame_does(tmp_path):
    scribe = create_scribe(str(tmp_path))
    df = pd.DataFrame({"a": [1, 2]})

    scribe.concat(df, "match-a")
    df["a"] = [3, 4]

    assert scribe.dataframe["a"].tolist() == [1, 2]


def test_page_size_counts_string_contents(tmp_path):
    scribe = create_scribe(str(tmp_path))
    text = "x" * 1024

    scribe.concat(pd.DataFrame({"text": [text] * 1024}), "match-a")

    # 1024 strings of 1 KiB each: at least 1 MiB, however pandas counts it.
    assert scribe.page_size_mb >= 1.0


def make_text_frame(rows, text_length, seed):
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {
            "tick": np.arange(rows, dtype="int32"),
            "x": rng.random(rows),
            "name": [f"{i:06d}".ljust(text_length, "x") for i in range(rows)],
            "weapon": rng.choice(WEAPONS, rows),
        }
    )


def deep_size_mb(frames):
    page = pd.concat(frames, ignore_index=True)
    return page.memory_usage(index=False, deep=True).sum() / 1024 / 1024


def test_page_size_is_the_deep_size_of_the_page_so_far(tmp_path):
    scribe = create_scribe(str(tmp_path))
    frames = [make_text_frame(50 + 10 * i, 20 + i, i) for i in range(12)]

    for i, frame in enumerate(frames):
        scribe.concat(frame, f"match-{i}")
        # Check at uneven intervals, so some checks see several new frames.
        if i % 5 in (0, 3):
            assert scribe.page_size_mb == deep_size_mb(frames[: i + 1])

    assert scribe.page_size_mb == deep_size_mb(frames)


def test_page_size_after_reading_the_page_mid_way(tmp_path):
    scribe = create_scribe(str(tmp_path))
    frames = [make_text_frame(40, 30 + i, i) for i in range(6)]

    for i, frame in enumerate(frames[:4]):
        scribe.concat(frame, f"match-{i}")
    assert scribe.page_size_mb == deep_size_mb(frames[:4])
    scribe.concat(frames[4], "match-4")
    # Reading the page merges the unmeasured frame into it.
    assert len(scribe.dataframe) == 5 * 40
    scribe.concat(frames[5], "match-5")

    assert scribe.page_size_mb == deep_size_mb(frames)


def test_string_heavy_pages_split_on_size(tmp_path):
    root_path = str(tmp_path)
    # 256 strings of 1 KiB: at least 0.25 MiB a frame, however pandas stores
    # strings. The numbers alone (int32 and float64) are 3 KiB a frame.
    frames = [make_text_frame(256, 1024, i) for i in range(10)]
    assert 0.25 <= deep_size_mb(frames[:1]) < 0.3
    scribe = create_scribe(root_path, max_page_size_mb=0.9, limit_check_frequency=1)

    scribe.start()
    for i, frame in enumerate(frames):
        scribe.concat(frame, f"match-{i}")
    scribe.finish()

    loader = create_loader(root_path)
    pages = list(loader.iterate_pages())
    assert [len(keyset) for _, keyset in pages] == [4, 4, 2]
    pd.testing.assert_frame_equal(
        pd.concat([df for df, _ in pages], ignore_index=True),
        pd.concat(frames, ignore_index=True),
    )


def test_pages_split_on_row_count(tmp_path):
    root_path = str(tmp_path)
    frames = [make_frame(i) for i in range(3)]
    scribe = create_scribe(root_path, max_page_row_count=3, limit_check_frequency=1)

    scribe.start()
    for i, frame in enumerate(frames):
        scribe.concat(frame, f"match-{i}")
    scribe.finish()

    loader = create_loader(root_path)
    pages = list(loader.iterate_pages())
    assert [keyset for _, keyset in pages] == [["match-0", "match-1"], ["match-2"]]
    pd.testing.assert_frame_equal(
        pd.concat([df for df, _ in pages], ignore_index=True),
        pd.concat(frames, ignore_index=True),
    )


def test_tome_name_comes_from_the_manifest(tmp_path):
    scribe = create_scribe(str(tmp_path))

    assert scribe.tome_name == TOME_NAME
