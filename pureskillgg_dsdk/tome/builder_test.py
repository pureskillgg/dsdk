# pylint: disable=missing-docstring,redefined-outer-name
import gzip
import json
import os
import shutil

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ..ds_io import DsReaderFs
from . import builder
from .builder import MATCH_KEY_COLUMN, build_basic_tomes_from_fs
from .constants import get_page_path_fs
from .curator import TomeCuratorFs

DS_TYPE = "csds"
FIXTURES = "fixtures"
HEADER = "header.2022-01-01,2022-01-02"
# Every fixture channel but the three largest (tick, player_status and
# player_vector), to keep the suite quick.
FIXTURE_CHANNELS = [
    "bomb_action",
    "bomb_defuse",
    "bomb_state",
    "bot_takeover",
    "grenade_state",
    "item_equip",
    "item_pickup",
    "item_remove",
    "molotov_state",
    "player_action",
    "player_blind",
    "player_connect",
    "player_death",
    "player_disconnect",
    "player_fall",
    "player_footstep",
    "player_hurt",
    "player_info",
    "player_interaction",
    "player_name",
    "player_personal",
    "player_spawn",
    "round_end",
    "round_mvp",
    "round_start",
    "round_state",
    "weapon_action",
    "weapon_fire",
]


class RecordingLogger:
    """A structlog-shaped logger that keeps every event."""

    def __init__(self):
        self.events = []

    def bind(self, **_):
        return self

    def __getattr__(self, level):
        def log(event=None, **kw):
            self.events.append((level, event, kw))

        return log

    def named(self, event):
        return [kw for _, name, kw in self.events if name == event]


def make_curator(tome_root, ds_root=FIXTURES, log=None):
    return TomeCuratorFs(
        default_header_name=HEADER,
        ds_type=DS_TYPE,
        tome_collection_root_path=str(tome_root),
        ds_collection_root_path=str(ds_root),
        log=log,
    )


def build_the_old_way(  # pylint: disable=too-many-locals
    curator, channels, *, prefix="old", **make_tome_kwargs
):
    """
    make_standard_tomes.py's way: create_header_tome, a subheader of the
    matches that have each channel, and a make_tome over it.
    """
    curator.create_header_tome(HEADER)
    header = curator.get_header_loader().get_dataframe()
    have = {channel: set() for channel in channels}
    for key in header["key"]:
        reader = DsReaderFs(
            root_path=curator._ds_collection_root_path,  # pylint: disable=protected-access
            manifest_key=key,
        )
        for entry in reader.read_manifest()["channels"]:
            if entry["channel"] in have:
                have[entry["channel"]].add(key)
    tomes = {}
    for channel in channels:
        if not have[channel]:
            continue
        subheader = f"has_{channel}.2022-01-01,2022-01-02"
        curator.create_subheader_tome(
            subheader, lambda df, keys=have[channel]: df["key"].isin(keys)
        )
        name = f"{prefix}_{channel}.2022-01-01,2022-01-02"
        maker = curator.make_tome(
            name,
            header_tome_name=subheader,
            ds_reading_instructions=[{"channel": channel}],
            **make_tome_kwargs,
        )
        for data, key in maker.iterate():
            df = data[channel]
            df["match_key"] = key
            maker.concat(df)
        tomes[channel] = curator.get_loader(name)
    return tomes


def frame(loader):
    return loader.get_dataframe().reset_index(drop=True)


def assert_same_tome(new, old):
    assert new.manifest["isComplete"] is True
    assert new.get_keyset() == old.get_keyset()
    pd.testing.assert_frame_equal(frame(new), frame(old))
    assert new.header.get_keyset() == old.header.get_keyset()
    pd.testing.assert_frame_equal(frame(new.header), frame(old.header))


def page_frames(loader):
    return [(df.reset_index(drop=True), keys) for df, keys in loader.iterate_pages()]


# A synthetic collection: CSDS-shaped matches written by pandas, as ppp does.


def header_frame(day, map_name="de_dust2"):
    return pd.DataFrame(
        {
            "map_name": [map_name],
            "match_date": [f"2022-05-{day:02d}"],
            "platform": ["steam"],
        }
    )


def write_match(root, name, channels, *, day=15):
    """One match: a gzipped JSON manifest and one parquet file per channel."""
    key_dir = f"csds/2022/05/{day:02d}/{name}"
    folder = os.path.join(str(root), *key_dir.split("/"))
    os.makedirs(folder)
    entries = []
    for channel, data in {"header": header_frame(day), **channels}.items():
        path = os.path.join(folder, channel)
        if isinstance(data, pa.Table):
            pq.write_table(data, path)
            columns = data.column_names
        else:
            data.to_parquet(path)
            columns = list(data.columns)
        entries.append(
            {
                "channel": channel,
                "key": f"{key_dir}/{channel}",
                "contentType": "application/x-parquet",
                "columns": [{"name": c} for c in columns],
            }
        )
    manifest = {"id": f"id-{name}", "key": f"{key_dir}/csds", "channels": entries}
    with gzip.open(os.path.join(folder, DS_TYPE), "wb") as file:
        file.write(json.dumps(manifest).encode("utf8"))
    return f"{key_dir}/csds"


def deaths(rows, seed, *, player_id_fixed="int64", player_id="Int64", **extra):
    """A player_death-like channel, with the type drifts seen across csds versions."""
    rng = np.random.default_rng(seed)
    data = {
        "round": pd.array(rng.integers(1, 30, rows), dtype="Int64"),
        "tick": rng.integers(0, 100_000, rows),
        "player_id": pd.array(rng.integers(0, 10, rows), dtype=player_id),
        "player_id_fixed": rng.integers(0, 10, rows).astype(player_id_fixed),
        "weapon": rng.choice(["ak47", "awp", "m4a1"], rows),
        "is_headshot": rng.random(rows) < 0.3,
        "x": rng.random(rows),
    }
    data.update(extra)
    return pd.DataFrame(data)


def empty_channel(*columns):
    """An empty channel as ppp writes it: object columns, stored as null type."""
    return pd.DataFrame({c: pd.Series([], dtype=object) for c in columns})


@pytest.fixture
def drift_collection(tmp_path):
    """Matches whose channels drift in type and columns, as the CSDS versions do."""
    root = tmp_path / "ds"
    write_match(root, "m00", {"player_death": deaths(40, 0), "round_end": rounds(3)})
    write_match(
        root,
        "m01",
        {
            # player_id_fixed stored as double, the id declared int64.
            "player_death": deaths(25, 1, player_id_fixed="float64", player_id="int64"),
            "round_end": rounds(4),
        },
        day=16,
    )
    # The channel is missing from this match.
    write_match(root, "m02", {"round_end": rounds(2)}, day=16)
    write_match(
        root,
        "m03",
        {
            # A column added, one dropped, and the id declared object.
            "player_death": deaths(
                30,
                3,
                player_id="object",
                assister_id=pd.array([None] * 30, dtype="Int64"),
            ).drop(columns=["x"]),
            "round_end": rounds(3),
        },
        day=17,
    )
    # An empty file with null-typed columns.
    write_match(
        root,
        "m04",
        {
            "player_death": empty_channel("round", "tick", "player_id"),
            "round_end": rounds(1),
        },
        day=17,
    )
    stored_index = deaths(10, 5).set_axis(range(100, 110))
    write_match(
        root,
        "m05",
        {"player_death": stored_index, "round_end": rounds(2)},
        day=18,
    )
    return root


def rounds(n):
    return pd.DataFrame(
        {
            "round": np.arange(1, n + 1),
            "winner": np.where(np.arange(n) % 2 == 0, "t", "ct"),
            "reason_code": pd.array(range(n), dtype="Int64"),
        }
    )


# Equivalence with make_tome.


@pytest.mark.parametrize("read_threads", [0, 4])
def test_fixture_tomes_equal_make_tome(tmp_path, read_threads):
    curator = make_curator(tmp_path)
    old = build_the_old_way(curator, FIXTURE_CHANNELS)

    built = curator.build_basic_tomes(
        FIXTURE_CHANNELS,
        tome_name="new_{channel}.{dates}",
        header_tome_name="new_header.2022-01-01,2022-01-02",
        max_page_size_mb=0.05,
        read_threads=read_threads,
    )

    assert built.dates == "2022-05-14,2022-05-15"
    assert set(built.tomes) == set(old)
    assert built.header.get_keyset() == curator.get_header_loader().get_keyset()
    pd.testing.assert_frame_equal(
        frame(built.header), frame(curator.get_header_loader())
    )
    for channel, loader in built.tomes.items():
        assert loader.manifest["tome"] == f"new_{channel}.2022-05-14,2022-05-15"
        assert_same_tome(loader, old[channel])
    # Channels with rows carry match_key; some fixture channels have none.
    assert MATCH_KEY_COLUMN in frame(built.tomes["player_death"]).columns
    # Small pages: some channels span several.
    assert max(len(loader.manifest["pages"]) for loader in built.tomes.values()) > 1


@pytest.mark.parametrize("compact_every", [256, 2, 1])
@pytest.mark.parametrize("max_page_size_mb", [None, 0.001])
def test_drifting_schemas_equal_make_tome(
    tmp_path, drift_collection, max_page_size_mb, compact_every, monkeypatch
):
    # Merging a page's tables as it fills gives the same page.
    monkeypatch.setattr(builder, "_COMPACT_EVERY", compact_every)
    log = RecordingLogger()
    curator = make_curator(tmp_path / "tomes", drift_collection, log=log)
    old = build_the_old_way(
        curator,
        ["player_death", "round_end"],
        max_page_size_mb=max_page_size_mb,
        limit_check_frequency=1,
    )

    built = curator.build_basic_tomes(
        ["player_death", "round_end"],
        tome_name="new_{channel}.{dates}",
        header_tome_name="new_header.2022-01-01,2022-01-02",
        max_page_size_mb=max_page_size_mb,
    )

    for channel in ["player_death", "round_end"]:
        assert_same_tome(built.tomes[channel], old[channel])
    deaths_frame = frame(built.tomes["player_death"])
    dtypes = deaths_frame.dtypes
    # Int64 wins over int64 and object; int64 with double becomes double.
    assert dtypes["player_id"] == "Int64"
    assert dtypes["round"] == "Int64"
    assert dtypes["player_id_fixed"] == "float64"
    assert dtypes["assister_id"] == "Int64"
    assert "__index_level_0__" not in deaths_frame.columns
    # The match without the channel is left out; the empty one is kept.
    keys = built.tomes["player_death"].get_keyset()
    assert [k.split("/")[4] for k in keys] == ["m00", "m01", "m03", "m04", "m05"]
    assert len(built.tomes["round_end"].get_keyset()) == 6
    # pyarrow built every page; none needed pandas.
    assert log.named("Page built with pandas") == []


def test_header_equals_create_header_tome(tmp_path, drift_collection):
    curator = make_curator(tmp_path / "tomes", drift_collection)
    curator.create_header_tome(HEADER)

    built = curator.build_basic_tomes(
        ["round_end"], header_tome_name="new_header.2022-01-01,2022-01-02"
    )

    old = curator.get_header_loader()
    assert built.header.manifest["isHeader"] is True
    assert built.header.get_keyset() == old.get_keyset()
    pd.testing.assert_frame_equal(frame(built.header), frame(old))
    assert built.dates == "2022-05-15,2022-05-18"


def test_columns_instruction(tmp_path, drift_collection):
    curator = make_curator(tmp_path / "tomes", drift_collection)
    instruction = {"channel": "round_end", "columns": ["winner", "round"]}

    built = curator.build_basic_tomes([instruction])

    df = frame(built.tomes["round_end"])
    assert list(df.columns) == ["winner", "round", MATCH_KEY_COLUMN]


@pytest.mark.parametrize("compact_every", [256, 2, 1])
def test_page_built_with_pandas_when_arrow_cannot_match_it(
    tmp_path, compact_every, monkeypatch
):
    # pyarrow joins differing categories into one categorical; pandas makes
    # them object, so this page is built with pandas.
    monkeypatch.setattr(builder, "_COMPACT_EVERY", compact_every)
    root = tmp_path / "ds"
    for i, categories in enumerate([["x", "y"], ["z"]]):
        channel = pd.DataFrame(
            {"kind": pd.Categorical(categories), "n": [i] * len(categories)}
        )
        write_match(root, f"m{i}", {"events": channel}, day=15 + i)
    log = RecordingLogger()
    curator = make_curator(tmp_path / "tomes", root, log=log)
    old = build_the_old_way(curator, ["events"])

    built = curator.build_basic_tomes(
        ["events"],
        tome_name="new_{channel}.{dates}",
        header_tome_name="h.2022-01-01,2022-01-02",
    )

    assert_same_tome(built.tomes["events"], old["events"])
    assert not isinstance(
        frame(built.tomes["events"])["kind"].dtype, pd.CategoricalDtype
    )
    assert len(log.named("Page built with pandas")) == 1


# Keys, pages and threads.


def test_duplicate_keys_are_built_once(tmp_path, drift_collection):
    log = RecordingLogger()
    curator = make_curator(tmp_path / "tomes", drift_collection, log=log)
    keys = builder.get_manifest_key_paths_from_glob(str(drift_collection), DS_TYPE)
    keys = [k.replace(os.sep, "/") for k in keys]

    built = curator.build_basic_tomes(
        ["round_end"], keys=[keys[0], keys[1], keys[0], keys[2], keys[1]]
    )

    assert built.header.get_keyset() == keys[:3]
    assert built.tomes["round_end"].get_keyset() == keys[:3]
    assert log.named("Duplicate match keys dropped") == [{"count": 2}]


def test_duplicate_keys_in_a_kept_header_are_built_once(tmp_path, drift_collection):
    log = RecordingLogger()
    curator = make_curator(tmp_path / "tomes", drift_collection, log=log)
    curator.create_header_tome()
    header = curator.get_header_loader()
    doubled = pd.concat([frame(header), frame(header).iloc[:2]], ignore_index=True)
    curator.create_subheader_tome(
        "doubled.2022-01-01,2022-01-02", lambda df: [True] * len(df)
    )
    # Write the header again with two keys listed twice, as the July header had.
    loader = curator.get_loader("doubled.2022-01-01,2022-01-02")
    page = loader.manifest["pages"][0]
    doubled.to_parquet(get_page_path_fs(str(tmp_path / "tomes"), "dataframe", page))
    pd.DataFrame({"_": doubled["key"]}).to_parquet(
        get_page_path_fs(str(tmp_path / "tomes"), "keyset", page)
    )

    built = curator.build_basic_tomes(
        ["round_end"], header_tome_name="doubled.2022-01-01,2022-01-02"
    )

    keys = header.get_keyset()
    assert built.tomes["round_end"].get_keyset() == keys
    assert built.tomes["round_end"].header.get_keyset() == keys
    assert log.named("Duplicate match keys dropped") == [{"count": 2}]


def arrow_bytes(ds_root, key, channel):
    """The Arrow bytes a match adds to a channel's page."""
    manifest = DsReaderFs(root_path=str(ds_root), manifest_key=key).read_manifest()
    entry = next(e for e in manifest["channels"] if e["channel"] == channel)
    path = os.path.join(str(ds_root), os.path.normpath(entry["key"]))
    prepared = builder.prepare_table(pq.read_table(path), [(MATCH_KEY_COLUMN, key)])
    return 0 if prepared is None else prepared[0].nbytes


def test_pages_split_on_arrow_bytes(tmp_path, drift_collection):
    curator = make_curator(tmp_path / "tomes", drift_collection)
    whole = curator.build_basic_tomes(
        ["player_death"], tome_name="whole_{channel}.{dates}", max_page_size_mb=None
    ).tomes["player_death"]
    max_page_size_mb = 0.004

    split = curator.build_basic_tomes(
        ["player_death"],
        tome_name="split_{channel}.{dates}",
        max_page_size_mb=max_page_size_mb,
    ).tomes["player_death"]

    # A page is cut after the match that takes it past the limit.
    expected, page, size = [], [], 0
    for key in whole.get_keyset():
        page.append(key)
        size += arrow_bytes(drift_collection, key, "player_death")
        if size > max_page_size_mb * 1024 * 1024:
            expected.append(page)
            page, size = [], 0
    if page:
        expected.append(page)
    assert len(whole.manifest["pages"]) == 1
    assert [keys for _, keys in page_frames(split)] == expected
    assert 1 < len(expected) < len(whole.get_keyset())
    pd.testing.assert_frame_equal(frame(split), frame(whole))


def test_threads_give_the_same_pages(tmp_path):
    runs = {}
    for threads in [0, 4]:
        curator = make_curator(tmp_path / f"t{threads}")
        built = curator.build_basic_tomes(
            ["player_death", "player_hurt", "round_end"],
            max_page_size_mb=0.02,
            read_threads=threads,
        )
        runs[threads] = {c: page_frames(loader) for c, loader in built.tomes.items()}

    for channel, pages in runs[0].items():
        assert len(pages) == len(runs[4][channel])
        for (df0, keys0), (df4, keys4) in zip(pages, runs[4][channel]):
            assert keys0 == keys4
            pd.testing.assert_frame_equal(df0, df4)


def test_pages_default_to_zstd(tmp_path):
    curator = make_curator(tmp_path)
    built = curator.build_basic_tomes(["round_end"])

    for loader in [
        built.header,
        built.tomes["round_end"],
        built.tomes["round_end"].header,
    ]:
        for page in loader.manifest["pages"]:
            path = get_page_path_fs(str(tmp_path), "dataframe", page)
            assert (
                pq.ParquetFile(path).metadata.row_group(0).column(0).compression
                == "ZSTD"
            )


def test_channel_in_no_match_gets_no_tome(tmp_path, drift_collection):
    log = RecordingLogger()
    curator = make_curator(tmp_path / "tomes", drift_collection, log=log)

    built = curator.build_basic_tomes(["round_end", "player_fall"])

    assert set(built.tomes) == {"round_end"}
    assert not curator.get_loader("basic_player_fall." + built.dates).exists
    assert log.named("Channel in no matches: no tome") == [{"channel": "player_fall"}]


# Resuming.


def tome_ids(built):
    return {c: loader.manifest["id"] for c, loader in built.tomes.items()} | {
        "header": built.header.manifest["id"]
    }


def test_complete_tomes_are_kept_without_reading_matches(tmp_path, drift_collection):
    curator = make_curator(tmp_path / "tomes", drift_collection)
    first = curator.build_basic_tomes(["player_death", "round_end"])
    first_ids = tome_ids(first)
    shutil.rmtree(drift_collection)

    again = curator.build_basic_tomes(["player_death", "round_end"])

    assert tome_ids(again) == first_ids
    assert again.dates == first.dates


def test_overwrite_builds_everything_again(tmp_path, drift_collection):
    curator = make_curator(tmp_path / "tomes", drift_collection)
    first = curator.build_basic_tomes(["player_death", "round_end"])
    first_ids = tome_ids(first)
    first_deaths = frame(first.tomes["player_death"])

    again = curator.build_basic_tomes(
        ["player_death", "round_end"], behavior_if_complete="overwrite"
    )

    for name, tome_id in tome_ids(again).items():
        assert tome_id != first_ids[name]
    pd.testing.assert_frame_equal(frame(again.tomes["player_death"]), first_deaths)


def test_fail_raises_on_a_complete_tome(tmp_path, drift_collection):
    curator = make_curator(tmp_path / "tomes", drift_collection)
    curator.build_basic_tomes(["round_end"])

    with pytest.raises(Exception, match="Tome already exists"):
        curator.build_basic_tomes(["round_end"], behavior_if_complete="fail")


def interrupt_after(monkeypatch, reads):
    """Make the walk stop with an error after this many channel files."""
    real = builder.read_channel
    count = {"n": 0}

    def read_channel(*args):
        count["n"] += 1
        if count["n"] > reads:
            raise RuntimeError("interrupted")
        return real(*args)

    monkeypatch.setattr(builder, "read_channel", read_channel)


def staging(tome_root):
    return os.path.join(str(tome_root), "tome", DS_TYPE, builder.STAGING_FOLDER)


def test_an_interrupted_build_leaves_no_partial_tome(
    tmp_path, drift_collection, monkeypatch
):
    reference = make_curator(
        tmp_path / "reference", drift_collection
    ).build_basic_tomes(["player_death", "round_end"], max_page_size_mb=0.0001)
    curator = make_curator(tmp_path / "tomes", drift_collection)
    # Four matches in: the header row and two channels of each.
    interrupt_after(monkeypatch, 12)
    with pytest.raises(RuntimeError, match="interrupted"):
        curator.build_basic_tomes(
            ["player_death", "round_end"], max_page_size_mb=0.0001, read_threads=0
        )
    monkeypatch.undo()

    # Pages were written, but to the staging folder: no channel tome exists.
    assert len(os.listdir(os.path.join(staging(tmp_path / "tomes"), HEADER))) == 2
    for channel in ["player_death", "round_end"]:
        assert not curator.get_loader(f"basic_{channel}.{reference.dates}").exists
    assert curator.get_header_loader().is_complete is False

    built = curator.build_basic_tomes(
        ["player_death", "round_end"], max_page_size_mb=0.0001
    )

    for channel in ["player_death", "round_end"]:
        assert_same_tome(built.tomes[channel], reference.tomes[channel])
    assert not os.path.exists(staging(tmp_path / "tomes"))


def test_partial_header_is_built_again(tmp_path, drift_collection):
    curator = make_curator(tmp_path / "tomes", drift_collection)
    first = curator.build_basic_tomes(["round_end"])
    first_ids = tome_ids(first)
    manifest_path = os.path.join(
        str(tmp_path / "tomes"), *first.header.manifest["key"].split("/")
    )
    manifest = dict(first.header.manifest, isComplete=False)
    with open(manifest_path, "w", encoding="utf-8") as file:
        json.dump(manifest, file)

    again = curator.build_basic_tomes(["round_end"])

    assert again.header.manifest["isComplete"] is True
    assert again.header.manifest["id"] != first_ids["header"]
    # The channel tome, found complete under the same name, is kept.
    assert again.tomes["round_end"].manifest["id"] == first_ids["round_end"]
    assert not os.path.exists(staging(tmp_path / "tomes"))


def partial_make_tome(curator, name):
    """
    A make_tome over the whole header, stopped after the first match, as an
    interrupted make_standard_tomes.py run leaves one.
    """
    maker = curator.make_tome(
        name,
        ds_reading_instructions=[{"channel": "player_death"}],
        max_page_row_count=1,
        limit_check_frequency=1,
    )
    for data, key in maker.iterate():
        df = data["player_death"]
        df["match_key"] = key
        maker.concat(df)
        break
    return curator.get_loader(name).manifest["id"]


def test_a_partial_make_tome_tome_is_built_again(tmp_path, drift_collection):
    reference = make_curator(
        tmp_path / "reference", drift_collection
    ).build_basic_tomes(["player_death"])
    curator = make_curator(tmp_path / "tomes", drift_collection)
    curator.create_header_tome()
    name = "basic_player_death." + reference.dates
    partial_id = partial_make_tome(curator, name)

    built = curator.build_basic_tomes(["player_death"])

    assert built.tomes["player_death"].manifest["id"] != partial_id
    assert_same_tome(built.tomes["player_death"], reference.tomes["player_death"])


def test_a_partial_tome_can_be_left(tmp_path, drift_collection):
    curator = make_curator(tmp_path / "tomes", drift_collection)
    curator.create_header_tome()
    name = "basic_player_death.2022-05-15,2022-05-18"
    partial_id = partial_make_tome(curator, name)

    built = curator.build_basic_tomes(
        ["player_death", "round_end"], behavior_if_partial="pass"
    )

    assert built.tomes["player_death"].manifest["id"] == partial_id
    assert built.tomes["player_death"].manifest["isComplete"] is False
    assert built.tomes["round_end"].manifest["isComplete"] is True
    with pytest.raises(Exception, match="Tome already exists"):
        curator.build_basic_tomes(["player_death"], behavior_if_partial="fail")


def test_kept_header_must_hold_the_keys(tmp_path, drift_collection):
    curator = make_curator(tmp_path / "tomes", drift_collection)
    built = curator.build_basic_tomes(["round_end"])
    keys = built.header.get_keyset()

    curator.build_basic_tomes(["round_end"], keys=list(reversed(keys)))
    with pytest.raises(ValueError, match="keys differ"):
        curator.build_basic_tomes(["round_end"], keys=keys[:2])


# Arguments.


@pytest.mark.parametrize(
    "kwargs,message",
    [
        ({"tome_name": "basic.{dates}"}, "must contain {channel}"),
        ({"tome_name": "{channel}.{date}"}, "may only use"),
        ({"behavior_if_complete": "skip"}, "behavior_if_complete must be one of"),
        ({"behavior_if_partial": "continue"}, "behavior_if_partial must be one of"),
        ({"behavior_if_complete": "continue"}, "behavior_if_complete must be one of"),
        ({"read_threads": -1}, "read_threads"),
        ({"max_page_size_mb": 0}, "max_page_size_mb"),
        ({"compression": "zstdd"}, "Unsupported parquet compression"),
    ],
)
def test_bad_arguments_fail_before_any_read(tmp_path, kwargs, message):
    with pytest.raises(ValueError, match=message):
        build_basic_tomes_from_fs(
            ["round_end"],
            tome_collection_root_path=str(tmp_path),
            ds_collection_root_path="does-not-exist",
            header_tome_name=HEADER,
            **kwargs,
        )


def test_channels_must_be_a_list(tmp_path):
    with pytest.raises(ValueError, match="list"):
        make_curator(tmp_path).build_basic_tomes("round_end")


def test_dates_need_a_match_date(tmp_path):
    root = tmp_path / "ds"
    key = write_match(root, "m0", {"round_end": rounds(2)})
    header_path = os.path.join(str(root), *key.split("/")[:-1], "header")
    pd.DataFrame({"map_name": ["de_nuke"]}).to_parquet(header_path)
    curator = make_curator(tmp_path / "tomes", root)

    with pytest.raises(ValueError, match="no match_date"):
        curator.build_basic_tomes(["round_end"])
    built = curator.build_basic_tomes(
        ["round_end"], tome_name="basic_{channel}.2022-01-01,2022-01-02"
    )
    assert built.dates is None
    assert set(built.tomes) == {"round_end"}
