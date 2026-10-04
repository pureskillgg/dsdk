# pylint: disable=missing-docstring,redefined-outer-name
"""
Tomes that mix csds player tables in the old format (int64, float64, string
place_name, pandas metadata) with the compact one (int8 to int32, float32, a
dictionary place_name, rows by player then tick, no pandas metadata).

The compact files are derived from dsdk's own fixtures by `compact`, which
stores a table the way csgo-ppp's compact writer does.
"""

import glob
import json
import os
import warnings

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from . import builder
from .builder_test import FIXTURES, RecordingLogger, make_curator, write_match
from .constants import MATCH_KEY_COLUMN, get_page_path_fs
from .loader_test import arrow_only, concat_pages, frame, ints, write_tome, zstd
from .narrowing import (
    NarrowingError,
    narrow_numpy_type,
    narrow_target,
    widen_dtype,
)

ROWS = 1500
CHANNELS = ["player_vector", "player_status"]
FIXTURE_MATCHES = sorted(glob.glob(os.path.join(FIXTURES, "csds", "*", "*", "*", "*")))

# The compact integer types, from the csds player tables plan. Every other
# integer is int8, every float float32, and bools stay bool.
COMPACT_TYPES = {
    "tick": pa.int32(),
    "round": pa.int16(),
    "player_id": pa.int32(),
    "player_id_fixed": pa.int8(),
    # player_vector
    "current_ammo": pa.int16(),
    "weapon_code": pa.int16(),
    "view_punch_angle_tick": pa.int32(),
    "team_code": pa.int8(),
    # player_status
    "player_controller_id": pa.int8(),
    "inv_primary": pa.int16(),
    "inv_secondary": pa.int16(),
    "current_equipment_cost": pa.int16(),
    "freezetime_end_equipment_cost": pa.int16(),
    "money": pa.int16(),
    "ping": pa.int16(),
    "round_start_equipment_cost": pa.int16(),
    "equipment_value_calc": pa.int16(),
    # Flags: 0 and 1 in int64 columns declared Int64 in old files.
    "burst_mode": pa.bool_(),
    "is_silenced": pa.bool_(),
}


def fixture_table(match, channel, start=0):
    """
    ROWS rows of a fixture channel as stored: int64, float64, pandas
    metadata, and the flags burst_mode and is_silenced as 0 and 1 declared
    Int64, as the archive holds them.
    """
    return pq.read_table(os.path.join(match, channel)).slice(start, ROWS)


def compact(table, channel):
    """The table as csgo-ppp's compact writer stores it."""
    table = table.drop_columns(
        [n for n in table.column_names if n.startswith("__index_level_")]
    )
    columns = {}
    for field in table.schema:
        column = table.column(field.name)
        if field.name == "place_name":
            column = column.dictionary_encode()
        elif pa.types.is_floating(field.type):
            column = column.cast(pa.float32())
        elif pa.types.is_integer(field.type):
            column = column.cast(COMPACT_TYPES.get(field.name, pa.int8()))
        columns[field.name] = column
        if field.name == "player_id" and channel == "player_status":
            # The fixtures predate player_controller_id; compact files have it.
            columns["player_controller_id"] = column.cast(pa.int8())
    return pa.table(columns).sort_by(
        [("player_id", "ascending"), ("tick", "ascending")]
    )


def with_value(table, name, row, value, dtype=None):
    """The table with one value replaced, the column cast to `dtype` first."""
    values = (
        table.column(name)
        .to_numpy()
        .astype(dtype or table.column(name).type.to_pandas_dtype())
    )
    values[row] = value
    position = table.column_names.index(name)
    return table.set_column(position, name, pa.array(values))


def to_python(values):
    return [None if pd.isna(value) else value for value in values]


def arrow_type(dtype):
    """The Arrow type a pandas dtype's values convert to exactly, or None."""
    if pd.api.types.is_integer_dtype(dtype) or pd.api.types.is_float_dtype(dtype):
        numpy_dtype = np.dtype(getattr(dtype, "numpy_dtype", dtype))
        return pa.from_numpy_dtype(numpy_dtype)
    return None


@pytest.fixture
def sources():
    """The fixture tables each match of the mixed collection is written from."""
    first, second, third = FIXTURE_MATCHES
    old = {ch: fixture_table(first, ch) for ch in CHANNELS}
    # An old file with player_id_fixed as float64, as some csds versions
    # stored it, and current_ammo's 4294967295 (an empty magazine) on 5 rows.
    old_float = {ch: fixture_table(second, ch) for ch in CHANNELS}
    for ch in CHANNELS:
        old_float[ch] = old_float[ch].set_column(
            old_float[ch].column_names.index("player_id_fixed"),
            "player_id_fixed",
            old_float[ch].column("player_id_fixed").cast(pa.float64()),
        )
    vector = old_float["player_vector"]
    ammo = vector.column("current_ammo").to_numpy().copy()
    ammo[:5] = 4294967295
    old_float["player_vector"] = vector.set_column(
        vector.column_names.index("current_ammo"), "current_ammo", pa.array(ammo)
    )
    new = {ch: compact(fixture_table(third, ch), ch) for ch in CHANNELS}
    new_later = {ch: compact(fixture_table(first, ch, ROWS), ch) for ch in CHANNELS}
    # In key order: one page of an old match, one mixing a compact match and
    # an old one, and one of a compact match.
    return {"m0": old, "m1": new, "m2": old_float, "m3": new_later}


def write_collection(root, sources):
    return {
        name: write_match(root, name, channels) for name, channels in sources.items()
    }


def build(curator, channels=None, **kwargs):
    return curator.build_basic_tomes(
        channels or CHANNELS,
        tome_name="mixed_{channel}.{dates}",
        **kwargs,
    )


def declared_numpy_type(table, name):
    """The pandas dtype an old file's metadata declares for a column."""
    entries = json.loads(table.schema.metadata[b"pandas"])["columns"]
    return next(e["numpy_type"] for e in entries if e["name"] == name)


def expected_dtype(name, source_tables):
    """
    The dtype the mixed tome loads a column as: the compact type, nullable
    where an old file declares the column Int64 or lacks it; current_ammo
    stays as it was, Int64. None for the columns this doesn't cover.
    """
    if name == "current_ammo":
        return pd.Int64Dtype()
    old = [t for t in source_tables if t.schema.metadata]
    compact_tables = [t for t in source_tables if not t.schema.metadata]
    data_type = compact_tables[0].schema.field(name).type
    if pa.types.is_floating(data_type):
        return np.dtype("float32")
    nullable = any(
        name not in t.column_names or declared_numpy_type(t, name) == "Int64"
        for t in old
    )
    if pa.types.is_boolean(data_type):
        return pd.BooleanDtype() if nullable else np.dtype("bool")
    if not pa.types.is_integer(data_type):
        return None
    bits = data_type.bit_width
    return pd.api.types.pandas_dtype(f"Int{bits}" if nullable else f"int{bits}")


def match_rows(df, key):
    rows = df[df[MATCH_KEY_COLUMN] == key].reset_index(drop=True)
    return rows.drop(columns=MATCH_KEY_COLUMN)


def assert_rows_are_the_source(df, keys, sources, channel):
    """Each match's rows hold its file's values, cast to the tome's type."""
    for name, key in keys.items():
        source = sources[name][channel]
        rows = match_rows(df, key)
        assert len(rows) == source.num_rows
        for column in rows.columns:
            if column not in source.column_names:
                assert rows[column].isna().all(), column
                continue
            values = source.column(column)
            target = arrow_type(rows[column].dtype)
            if target is not None and values.type != target:
                values = values.cast(target, safe=False)
            if pa.types.is_dictionary(values.type):
                values = values.cast(values.type.value_type)
            assert to_python(rows[column].tolist()) == to_python(values.to_pylist()), (
                name,
                column,
            )


# Building a mixed tome.


def test_a_mixed_tome_holds_the_compact_types(tmp_path, sources):
    keys = write_collection(tmp_path / "ds", sources)
    log = RecordingLogger()
    curator = make_curator(tmp_path / "tomes", tmp_path / "ds", log=log)

    built = build(curator, max_page_size_mb=0.3)

    for channel in CHANNELS:
        loader = built.tomes[channel]
        # The pages are as the test means them: old, mixed, compact.
        assert [keys_ for _, keys_ in loader.iterate_pages()] == [
            [keys["m0"]],
            [keys["m1"], keys["m2"]],
            [keys["m3"]],
        ]
        compact_schema = sources["m1"][channel].schema
        for page in loader.manifest["pages"]:
            path = get_page_path_fs(str(tmp_path / "tomes"), "dataframe", page)
            schema = pq.read_schema(path)
            if channel == "player_vector":
                # current_ammo stays wide, on every page, the compact one too.
                assert schema.field("current_ammo").type == pa.int64()
            for field in compact_schema:
                if field.name in ("current_ammo", "place_name"):
                    # current_ammo is checked above; place_name is decoded.
                    continue
                if field.name == "player_controller_id" and page["number"] == 0:
                    # The old match lacks it.
                    assert field.name not in schema.names
                    continue
                assert schema.field(field.name).type == field.type, (
                    channel,
                    page["number"],
                    field.name,
                )
    # Each tome's old page was written wide, then narrowed once the compact
    # matches turned up; player_vector's compact page gets current_ammo int64.
    narrowed = sorted(
        (kw["tome"].rsplit("/", 1)[-1], kw["page_number"])
        for kw in log.named("Page narrowed")
    )
    assert narrowed == [
        ("player_status", 0),
        ("player_vector", 0),
        ("player_vector", 2),
    ]
    assert log.named("Page built with pandas") == []


def test_pages_built_with_pandas_are_narrowed_alike(tmp_path, sources, monkeypatch):
    write_collection(tmp_path / "ds", sources)
    with_arrow = build(
        make_curator(tmp_path / "arrow", tmp_path / "ds"), max_page_size_mb=0.3
    )
    # As when pyarrow can't join a page's tables: every page is built with
    # pandas, and narrowed the same way.
    monkeypatch.setattr(builder, "realize", lambda *args, **kwargs: None)
    log = RecordingLogger()
    with_pandas = build(
        make_curator(tmp_path / "pandas", tmp_path / "ds", log=log),
        max_page_size_mb=0.3,
    )

    by_pandas = [
        kw for kw in log.named("Page built with pandas") if "player_" in kw["tome"]
    ]
    assert len(by_pandas) == 6
    for channel in CHANNELS:
        pd.testing.assert_frame_equal(
            with_pandas.tomes[channel].get_dataframe(),
            with_arrow.tomes[channel].get_dataframe(),
        )


@pytest.mark.parametrize("channel", CHANNELS)
def test_a_mixed_tome_loads_narrow_and_unchanged(tmp_path, sources, channel):
    keys = write_collection(tmp_path / "ds", sources)
    curator = make_curator(tmp_path / "tomes", tmp_path / "ds")
    loader = build(curator, [channel], max_page_size_mb=0.3).tomes[channel]

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        df = loader.get_dataframe()

    tables = [s[channel] for s in sources.values()]
    for name in df.columns:
        expected = None if name == MATCH_KEY_COLUMN else expected_dtype(name, tables)
        if expected is not None:
            assert df[name].dtype == expected, name
    assert not isinstance(
        df.get("place_name", pd.Series(dtype=object)).dtype, pd.CategoricalDtype
    )
    assert_rows_are_the_source(df, keys, sources, channel)
    if channel == "player_vector":
        # current_ammo keeps the old files' 4294967295.
        assert (
            match_rows(df, keys["m2"])["current_ammo"].iloc[:5].tolist()
            == [4294967295] * 5
        )
    else:
        # The old matches lack player_controller_id: nulls, at the narrow type.
        assert df["player_controller_id"].dtype == "Int8"
        assert match_rows(df, keys["m0"])["player_controller_id"].isna().all()

    pl = pytest.importorskip("polars")
    polars_df = loader.get_dataframe(library="polars")
    for name in df.columns:
        assert to_python(polars_df[name].to_list()) == to_python(
            df[name].tolist()
        ), name
        if pd.api.types.is_integer_dtype(df[name].dtype):
            bits = (
                np.dtype(
                    getattr(df[name].dtype, "numpy_dtype", df[name].dtype)
                ).itemsize
                * 8
            )
            assert polars_df.schema[name] == getattr(pl, f"Int{bits}"), name
        if df[name].dtype == np.dtype("float32"):
            assert polars_df.schema[name] == pl.Float32, name


def test_widen_loads_the_same_values_at_64_bits(tmp_path, sources):
    write_collection(tmp_path / "ds", sources)
    curator = make_curator(tmp_path / "tomes", tmp_path / "ds")
    loader = build(curator, ["player_vector"], max_page_size_mb=0.3).tomes[
        "player_vector"
    ]

    narrow = loader.get_dataframe()
    wide = loader.get_dataframe(widen=True)

    assert narrow["weapon_code"].dtype == "Int16"
    assert narrow["x_pos"].dtype == "float32"
    for name in narrow.columns:
        dtype = wide[name].dtype
        if pd.api.types.is_integer_dtype(dtype):
            assert dtype in (np.dtype("int64"), pd.Int64Dtype()), name
        if pd.api.types.is_float_dtype(dtype):
            assert dtype == np.dtype("float64"), name
        assert to_python(wide[name].tolist()) == to_python(narrow[name].tolist()), name
    assert wide["weapon_code"].dtype == "Int64"
    assert wide["tick"].dtype == "int64"

    pl = pytest.importorskip("polars")
    schema = loader.scan(widen=True).collect_schema()
    assert schema["weapon_code"] == pl.Int64
    assert schema["x_pos"] == pl.Float64
    assert loader.scan().collect_schema()["x_pos"] == pl.Float32


def test_a_compact_tome_stays_on_pyarrow(tmp_path, sources):
    # place_name's dictionary is decoded, so the pages join with pyarrow.
    compact_sources = {"m1": sources["m1"], "m3": sources["m3"]}
    keys = write_collection(tmp_path / "ds", compact_sources)
    log = RecordingLogger()
    curator = make_curator(tmp_path / "tomes", tmp_path / "ds", log=log)

    built = build(curator, max_page_size_mb=None)

    assert log.named("Page built with pandas") == []
    assert log.named("Page narrowed") == []
    for channel in CHANNELS:
        df = built.tomes[channel].get_dataframe()
        for field in compact_sources["m1"][channel].schema:
            if pa.types.is_integer(field.type) or pa.types.is_floating(field.type):
                assert df[field.name].dtype == np.dtype(
                    field.type.to_pandas_dtype()
                ), field.name
        assert_rows_are_the_source(df, keys, compact_sources, channel)
    place_name = built.tomes["player_status"].get_dataframe()["place_name"]
    assert pd.api.types.is_string_dtype(place_name.dtype)
    assert not isinstance(place_name.dtype, pd.CategoricalDtype)


# Values that don't fit stop the build.

TOO_BIG = [
    ("player_status", "money", 40000, None),
    ("player_status", "health", -200, None),
    ("player_vector", "x_vel", 1e39, None),
    ("player_vector", "player_id_fixed", 1.5, "float64"),
]


@pytest.mark.parametrize("pages", ["one page", "the old page first"])
@pytest.mark.parametrize("channel,column,value,dtype", TOO_BIG)
def test_a_value_that_does_not_fit_stops_the_build(
    tmp_path, sources, pages, channel, column, value, dtype
):
    # In one page, the page is narrowed as it is built; with the old match on
    # a page of its own, that page is narrowed when the tome is finished.
    broken = dict(sources["m0"])
    broken[channel] = with_value(broken[channel], column, 7, value, dtype)
    keys = write_collection(tmp_path / "ds", {"m0": broken, "m1": sources["m1"]})
    curator = make_curator(tmp_path / "tomes", tmp_path / "ds")

    with pytest.raises(NarrowingError) as raised:
        build(
            curator,
            [channel],
            max_page_size_mb=None if pages == "one page" else 0.3,
        )

    message = str(raised.value)
    assert repr(column) in message
    assert repr(keys["m0"]) in message
    assert repr(value) in message


@pytest.mark.parametrize("old_value", [100, 200, 40000])
def test_the_widest_narrow_type_wins(tmp_path, old_value):
    # The first page joins an int8 compact match and an old one; a later
    # compact match brings int16. The tome is int16 on every page: 100 is
    # narrowed to int8 on the first page, then widened; 200 doesn't fit int8,
    # so that page keeps the column wide until int16 is known; 40000 fits
    # neither, and stops the build.
    root = tmp_path / "ds"
    rows = 1000
    old = pd.DataFrame(
        {"a": pd.array([old_value] + [1] * (rows - 1), dtype="Int64"), "n": 1}
    )
    keys = {
        "m0": write_match(
            root, "m0", {"events": pa.table({"a": pa.array([3, 4], pa.int8())})}
        ),
        "m1": write_match(root, "m1", {"events": old}),
        "m2": write_match(
            root, "m2", {"events": pa.table({"a": pa.array([300, 5], pa.int16())})}
        ),
    }
    log = RecordingLogger()
    curator = make_curator(tmp_path / "tomes", root, log=log)

    def build_events():
        return curator.build_basic_tomes(["events"], max_page_size_mb=0.01)

    if old_value == 40000:
        with pytest.raises(NarrowingError) as raised:
            build_events()
        assert repr(keys["m1"]) in str(raised.value)
        assert "40000" in str(raised.value)
        return
    loader = build_events().tomes["events"]

    assert [k for _, k in loader.iterate_pages()] == [
        [keys["m0"], keys["m1"]],
        [keys["m2"]],
    ]
    for page in loader.manifest["pages"]:
        path = get_page_path_fs(str(tmp_path / "tomes"), "dataframe", page)
        assert pq.read_schema(path).field("a").type == pa.int16()
    assert [kw["page_number"] for kw in log.named("Page narrowed")] == [0]
    df = loader.get_dataframe()
    assert df["a"].dtype == "Int16"
    assert df["a"].tolist() == [3, 4, old_value] + [1] * (rows - 1) + [300, 5]


def test_current_ammo_stays_wide(tmp_path, sources):
    # 4294967295 doesn't fit int16, and current_ammo is never narrowed.
    write_collection(tmp_path / "ds", {"m1": sources["m1"], "m2": sources["m2"]})
    curator = make_curator(tmp_path / "tomes", tmp_path / "ds")

    loader = build(curator, ["player_vector"], max_page_size_mb=None).tomes[
        "player_vector"
    ]

    df = loader.get_dataframe()
    assert df["current_ammo"].dtype == "Int64"
    assert (df["current_ammo"] == 4294967295).sum() == 5
    assert df["weapon_code"].dtype == "Int16"


# Flags: 0 and 1 declared Int64 in old files, bool in newer ones.

FLAG_ROWS = 1000


def flag_collection(root, old_flags):
    """An old match whose flag is old_flags then zeros, and a compact one."""
    old = pd.DataFrame(
        {
            "flag": pd.array(
                old_flags + [0] * (FLAG_ROWS - len(old_flags)), dtype="Int64"
            ),
            "n": 1,
        }
    )
    new = pa.table({"flag": pa.array([True, False]), "n": pa.array([2, 2])})
    return {
        "m0": write_match(root, "m0", {"events": old}),
        "m1": write_match(root, "m1", {"events": new}),
    }


@pytest.mark.parametrize("max_page_size_mb", [None, 0.01])
def test_flags_narrow_to_bool(tmp_path, max_page_size_mb):
    # With one page, the page is narrowed as it is built; with a page per
    # match, the old page is narrowed when the tome is finished.
    flag_collection(tmp_path / "ds", [1, None])
    log = RecordingLogger()
    curator = make_curator(tmp_path / "tomes", tmp_path / "ds", log=log)

    loader = curator.build_basic_tomes(
        ["events"], max_page_size_mb=max_page_size_mb
    ).tomes["events"]

    pages = loader.manifest["pages"]
    assert len(pages) == (1 if max_page_size_mb is None else 2)
    for page in pages:
        path = get_page_path_fs(str(tmp_path / "tomes"), "dataframe", page)
        assert pq.read_schema(path).field("flag").type == pa.bool_()
    built_with_pandas = log.named("Page built with pandas")
    assert [kw for kw in built_with_pandas if kw["tome"].endswith("events")] == []
    df = loader.get_dataframe()
    # Nullable, as the old file declared it; the missing value stays missing.
    assert df["flag"].dtype == "boolean"
    expected = [True, None] + [False] * (FLAG_ROWS - 2) + [True, False]
    assert to_python(df["flag"]) == expected
    assert loader.get_dataframe(widen=True)["flag"].dtype == "boolean"

    pl = pytest.importorskip("polars")
    polars_df = loader.get_dataframe(library="polars")
    assert polars_df.schema["flag"] == pl.Boolean
    assert to_python(polars_df["flag"].to_list()) == expected


@pytest.mark.parametrize("max_page_size_mb", [None, 0.01])
def test_a_flag_that_is_not_0_or_1_stops_the_build(tmp_path, max_page_size_mb):
    keys = flag_collection(tmp_path / "ds", [1, 2])
    curator = make_curator(tmp_path / "tomes", tmp_path / "ds")

    with pytest.raises(NarrowingError) as raised:
        curator.build_basic_tomes(["events"], max_page_size_mb=max_page_size_mb)

    message = str(raised.value)
    assert "'flag'" in message
    assert repr(keys["m0"]) in message
    assert "holds 2," in message


# Narrowing on load: a tome whose pages hold old and compact matches.


def old_page(**overrides):
    data = {
        "tick": [10, 20],
        "money": ints(800, None),
        "x": [1.5, np.nan],
        "player_id_fixed": [1.0, 2.0],
        "current_ammo": ints(4294967295, 30),
        "flag": [True, False],
        MATCH_KEY_COLUMN: ["old-match", "old-match"],
    }
    data.update(overrides)
    return frame(**data)


def compact_page():
    return frame(
        tick=np.array([30, 40], dtype="int32"),
        money=np.array([16000, 0], dtype="int16"),
        x=np.array([0.25, -3.0], dtype="float32"),
        player_id_fixed=np.array([3, 4], dtype="int8"),
        current_ammo=np.array([-1, 5], dtype="int16"),
        flag=[False, True],
        **{MATCH_KEY_COLUMN: ["new-match", "new-match"]},
    )


def test_pages_old_and_compact_load_narrow(tmp_path):
    loader = write_tome(
        str(tmp_path), [(zstd, old_page()), (arrow_only, compact_page())]
    )

    df = loader.get_dataframe()

    assert dict(df.dtypes) == {
        "tick": np.dtype("int32"),
        "money": pd.Int16Dtype(),
        "x": np.dtype("float32"),
        "player_id_fixed": np.dtype("int8"),
        "current_ammo": pd.Int64Dtype(),
        "flag": np.dtype("bool"),
        MATCH_KEY_COLUMN: df[MATCH_KEY_COLUMN].dtype,
    }
    assert to_python(df["money"]) == [800, None, 16000, 0]
    assert to_python(df["x"]) == [1.5, None, 0.25, -3.0]
    assert df["current_ammo"].tolist() == [4294967295, 30, -1, 5]
    selected = loader.get_dataframe(columns=["x", "money"])
    pd.testing.assert_frame_equal(selected, df[["x", "money"]])

    pl = pytest.importorskip("polars")
    polars_df = loader.get_dataframe(library="polars")
    assert polars_df.schema["money"] == pl.Int16
    assert polars_df.schema["x"] == pl.Float32
    assert polars_df.schema["player_id_fixed"] == pl.Int8
    assert polars_df.schema["current_ammo"] == pl.Int64
    for name in df.columns:
        assert to_python(polars_df[name].to_list()) == to_python(
            df[name].tolist()
        ), name
    polars_selected = loader.get_dataframe(columns=["x", "money"], library="polars")
    assert polars_selected.columns == ["x", "money"]
    assert polars_selected.schema["money"] == pl.Int16


def test_a_column_a_page_lacks_is_narrowed_with_nulls(tmp_path):
    # As player_controller_id: missing from some old files, Int64 in others,
    # int8 in compact ones.
    loader = write_tome(
        str(tmp_path),
        [
            (zstd, old_page().drop(columns="money")),
            (zstd, old_page()),
            (arrow_only, compact_page()),
        ],
    )

    df = loader.get_dataframe()

    assert df["money"].dtype == "Int16"
    assert to_python(df["money"]) == [None, None, 800, None, 16000, 0]

    pl = pytest.importorskip("polars")
    polars_df = loader.get_dataframe(library="polars")
    assert polars_df.schema["money"] == pl.Int16
    assert to_python(polars_df["money"].to_list()) == to_python(df["money"].tolist())


@pytest.mark.parametrize(
    "overrides,column,value",
    [
        ({"money": ints(800, 40000)}, "money", 40000),
        ({"x": [1.5, 1e39]}, "x", 1e39),
        ({"player_id_fixed": [1.0, 2.5]}, "player_id_fixed", 2.5),
    ],
)
def test_a_value_that_does_not_fit_fails_the_load(tmp_path, overrides, column, value):
    loader = write_tome(
        str(tmp_path), [(zstd, old_page(**overrides)), (arrow_only, compact_page())]
    )

    reads = [loader.get_dataframe]
    if _has_polars():
        reads += [lambda: loader.get_dataframe(library="polars"), loader.scan]
    for load in reads:
        with pytest.raises(NarrowingError) as raised:
            load()
        message = str(raised.value)
        assert repr(column) in message
        assert "'old-match'" in message
        assert repr(value) in message


def _has_polars():
    try:
        import polars  # pylint: disable=import-outside-toplevel,unused-import
    except ImportError:
        return False
    return True


def test_narrow_ints_with_missing_values_are_nullable(tmp_path):
    loader = write_tome(
        str(tmp_path),
        [
            (zstd, frame(a=[1, 2])),
            (arrow_only, frame(a=[3, 4], b=np.array([5, 6], dtype="int8"))),
        ],
    )

    df = loader.get_dataframe()

    # pd.concat would give float64; the column stays 8 bits.
    assert df["b"].dtype == "Int8"
    assert to_python(df["b"]) == [None, None, 5, 6]
    assert df["a"].dtype == "int64"


def test_a_compact_page_with_nulls_is_nullable(tmp_path):
    table = pa.table({"b": pa.array([1, None], pa.int8())})

    def write_the_table(_df, path):
        pq.write_table(table, path)

    loader = write_tome(str(tmp_path), [(write_the_table, frame(b=[0]))])

    df = loader.get_dataframe()

    assert df["b"].dtype == "Int8"
    assert to_python(df["b"]) == [1, None]


def test_old_tomes_widen_to_themselves(tmp_path):
    loader = write_tome(str(tmp_path), [(zstd, old_page()), (zstd, old_page())])

    narrow = loader.get_dataframe()
    wide = loader.get_dataframe(widen=True)

    pd.testing.assert_frame_equal(narrow, wide)
    assert narrow["money"].dtype == "Int64"
    assert narrow["player_id_fixed"].dtype == "float64"


def flag_pages(old_flags):
    return [
        (
            zstd,
            frame(
                flag=ints(*old_flags),
                **{MATCH_KEY_COLUMN: ["old-match"] * len(old_flags)},
            ),
        ),
        (
            arrow_only,
            frame(flag=[True, False], **{MATCH_KEY_COLUMN: ["new-match"] * 2}),
        ),
    ]


def test_flags_on_old_and_compact_pages_load_as_bool(tmp_path):
    loader = write_tome(str(tmp_path), flag_pages([1, None, 0]))

    df = loader.get_dataframe()

    assert df["flag"].dtype == "boolean"
    assert to_python(df["flag"]) == [True, None, False, True, False]
    assert loader.get_dataframe(widen=True)["flag"].dtype == "boolean"

    pl = pytest.importorskip("polars")
    polars_df = loader.get_dataframe(library="polars")
    assert polars_df.schema["flag"] == pl.Boolean
    assert to_python(polars_df["flag"].to_list()) == to_python(df["flag"])
    assert loader.scan(widen=True).collect_schema()["flag"] == pl.Boolean


def test_a_flag_that_is_not_0_or_1_fails_the_load(tmp_path):
    loader = write_tome(str(tmp_path), flag_pages([0, 2]))

    reads = [loader.get_dataframe]
    if _has_polars():
        reads += [lambda: loader.get_dataframe(library="polars"), loader.scan]
    for load in reads:
        with pytest.raises(NarrowingError) as raised:
            load()
        message = str(raised.value)
        assert "'flag'" in message
        assert "'old-match'" in message
        assert "holds 2," in message


@pytest.mark.parametrize(
    "pages",
    [
        [(zstd, frame(flag=ints(0, 1))), (zstd, frame(flag=ints(1, None)))],
        [(zstd, frame(flag=[True, False])), (arrow_only, frame(flag=[False]))],
    ],
    ids=["Int64 flags", "bool flags"],
)
def test_flags_of_one_format_load_as_before(tmp_path, pages):
    root_path = str(tmp_path)
    loader = write_tome(root_path, pages)

    pd.testing.assert_frame_equal(
        loader.get_dataframe(), concat_pages(root_path, loader)
    )


# The rules.


@pytest.mark.parametrize(
    "name,types,expected",
    [
        ("a", [pa.int64(), pa.int16()], pa.int16()),
        ("a", [pa.int64(), pa.int8(), pa.int16()], pa.int16()),
        ("a", [pa.float64(), pa.int8()], pa.int8()),
        ("a", [pa.float64(), pa.int64(), pa.int8()], pa.int8()),
        ("a", [pa.float64(), pa.float32()], pa.float32()),
        ("a", [pa.null(), pa.int64(), pa.int32()], pa.int32()),
        ("current_ammo", [pa.int64(), pa.int16()], pa.int64()),
        ("current_ammo", [pa.int16()], None),
        ("current_ammo", [pa.int64()], None),
        ("current_ammo", [pa.int64(), pa.float64()], None),
        ("a", [pa.int64()], None),
        ("a", [pa.int8(), pa.int16()], None),
        ("a", [pa.int64(), pa.float64()], None),
        ("a", [pa.null(), pa.int8()], None),
        ("a", [pa.bool_(), pa.int64()], pa.bool_()),
        ("a", [pa.bool_(), pa.int8()], pa.bool_()),
        ("a", [pa.bool_(), pa.int64(), pa.int8(), pa.null()], pa.bool_()),
        ("a", [pa.bool_()], None),
        ("a", [pa.bool_(), pa.null()], None),
        ("a", [pa.bool_(), pa.float64()], None),
        ("a", [pa.bool_(), pa.uint8()], None),
        ("a", [pa.bool_(), pa.string()], None),
        ("a", [pa.float32(), pa.int8(), pa.int64()], None),
        ("a", [pa.uint8(), pa.int64()], None),
        ("a", [pa.string(), pa.int16()], None),
    ],
)
def test_narrow_target(name, types, expected):
    assert narrow_target(name, types) == expected


@pytest.mark.parametrize(
    "numpy_type,target,has_nulls,expected",
    [
        ("Int64", pa.int16(), False, "Int16"),
        ("int64", pa.int16(), False, "int16"),
        ("int64", pa.int32(), True, "Int32"),
        ("float64", pa.int8(), False, "int8"),
        ("float64", pa.int8(), True, "Int8"),
        ("object", pa.int8(), False, "int8"),
        ("float64", pa.float32(), True, "float32"),
        ("Float64", pa.float32(), False, "Float32"),
        ("Int64", pa.bool_(), False, "boolean"),
        ("int64", pa.bool_(), False, "bool"),
        ("int64", pa.bool_(), True, "boolean"),
    ],
)
def test_narrow_numpy_type(numpy_type, target, has_nulls, expected):
    assert narrow_numpy_type(numpy_type, target, has_nulls) == expected


@pytest.mark.parametrize(
    "dtype,expected",
    [
        (np.dtype("int8"), np.dtype("int64")),
        (np.dtype("uint32"), np.dtype("int64")),
        (np.dtype("float32"), np.dtype("float64")),
        (pd.Int16Dtype(), pd.Int64Dtype()),
        (pd.Float32Dtype(), pd.Float64Dtype()),
        (np.dtype("int64"), None),
        (np.dtype("uint64"), None),
        (np.dtype("float64"), None),
        (pd.Int64Dtype(), None),
        (np.dtype("bool"), None),
        (pd.BooleanDtype(), None),
        (pd.CategoricalDtype(["x"]), None),
    ],
)
def test_widen_dtype(dtype, expected):
    assert widen_dtype(dtype) == expected
