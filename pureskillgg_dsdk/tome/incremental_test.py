# pylint: disable=missing-docstring,redefined-outer-name,unused-import
# pylint: disable=too-many-lines,too-many-arguments,too-many-positional-arguments
# pylint: disable=too-many-locals,protected-access
"""
Resuming an interrupted build and extending a finished one.

Every test holds a build that was resumed or extended to a build of all the
matches at once: the same keys in the same order, the same rows and the same
dtypes. Pages may be cut at other matches.
"""

import gzip
import hashlib
import json
import os
import shutil
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from . import builder, incremental
from .builder_test import (  # fixtures and helpers of the builder tests
    DS_TYPE,
    HEADER,
    RecordingLogger,
    assert_same_tome,
    drift_collection,
    frame,
    interrupt_after,
    make_curator,
    staging,
    write_match,
)
from .constants import get_page_path_fs
from .narrowing import NarrowingError
from .writer_fs import TomeWriterFs

# The tome names of an extendable collection have no {dates}.
BASIC = "basic_{channel}"
CHANNELS = ["player_death", "round_end"]
# A page for about every match; no threads, so that a stop falls on a known read.
SMALL = {"max_page_size_mb": 0.001, "read_threads": 0}
FIRST = ["m00", "m01", "m02"]
LATER = ["m03", "m04", "m05"]

pytestmark = pytest.mark.filterwarnings("ignore:Header name of")


# Helpers.


def build(curator, channels=None, **kwargs):
    return curator.build_basic_tomes(
        CHANNELS if channels is None else channels, **{**SMALL, **kwargs}
    )


def match_folders(root) -> dict:
    """Match name -> its folder under the collection, for the synthetic layout."""
    keys = builder.get_manifest_key_paths_from_glob(str(root), DS_TYPE)
    keys = [key.replace(os.sep, "/") for key in keys]
    return {key.split("/")[4]: "/".join(key.split("/")[:-1]) for key in keys}


def copy_matches(source, destination, names):
    """Copy matches from one collection to another: they 'arrive'."""
    folders = match_folders(source)
    for name in names:
        shutil.copytree(
            os.path.join(str(source), *folders[name].split("/")),
            os.path.join(str(destination), *folders[name].split("/")),
        )


def keys_of(root, names):
    folders = match_folders(root)
    return [folders[name] + "/" + DS_TYPE for name in names]


def count_reads(monkeypatch) -> list:
    """The channel of every file the builder reads from now on."""
    real = builder.read_channel
    reads = []

    def read_channel(path, columns, channel):
        reads.append(channel)
        return real(path, columns, channel)

    monkeypatch.setattr(builder, "read_channel", read_channel)
    return reads


def snapshot(root) -> dict:
    """Every file under root, but the staging folder, by its content."""
    files = {}
    for folder, folders, names in os.walk(str(root)):
        folders[:] = [name for name in folders if name != builder.STAGING_FOLDER]
        for name in names:
            path = os.path.join(folder, name)
            with open(path, "rb") as file:
                files[os.path.relpath(path, str(root))] = hashlib.sha256(
                    file.read()
                ).hexdigest()
    return files


def unlisted(root) -> list:
    """Page files that no manifest next to them lists."""
    stray = []
    for folder, folders, names in os.walk(str(root)):
        folders[:] = [name for name in folders if name != builder.STAGING_FOLDER]
        if "tome" not in names:
            continue
        with open(os.path.join(folder, "tome"), encoding="utf-8") as file:
            manifest = json.load(file)
        listed = {
            os.path.basename(page[subtype]["key"])
            for page in manifest["pages"]
            for subtype in ("dataframe", "keyset")
        }
        stray += [
            os.path.join(folder, name)
            for name in names
            if name != "tome" and name not in listed
        ]
    return stray


def assert_same_collection(built, reference):
    assert set(built.tomes) == set(reference.tomes)
    for channel, loader in built.tomes.items():
        assert_same_tome(loader, reference.tomes[channel])
    assert built.header.manifest["isComplete"] is True
    assert built.header.get_keyset() == reference.header.get_keyset()
    pd.testing.assert_frame_equal(frame(built.header), frame(reference.header))
    assert built.dates == reference.dates


def assert_same_rows(loader, other, channel):
    """The same rows, whatever their order and the order of the columns (matches came in another order)."""
    sort = (
        ["match_key", "tick"] if channel == "player_death" else ["match_key", "round"]
    )

    def rows(tome):
        df = frame(tome)
        return df[sorted(df.columns)].sort_values(
            sort, ignore_index=True, kind="stable"
        )

    pd.testing.assert_frame_equal(rows(loader), rows(other))


def page_types(loader, column) -> set:
    root = loader._reader._root_path  # pylint: disable=protected-access
    return {
        pq.read_schema(get_page_path_fs(root, "dataframe", page)).field(column).type
        for page in loader.manifest["pages"]
    }


@pytest.fixture
def arrival(tmp_path, drift_collection):
    """A collection of the first matches, and the rest of them to arrive later."""
    root = tmp_path / "growing"
    copy_matches(drift_collection, root, FIRST)
    return SimpleNamespace(
        root=root,
        full=drift_collection,
        arrive=lambda names=LATER: copy_matches(drift_collection, root, names),
    )


def events_collection(root):
    """
    Three matches of one channel, as test_the_widest_narrow_type_wins has
    them: m0 and m1 are int8 and an old int64 column that fits int8, m2 brings
    int16, so that the first pages are int8 until m2 comes.
    """
    old = pd.DataFrame({"a": pd.array([100] + [1] * 999, dtype="Int64"), "n": 1})
    write_match(root, "m0", {"events": pa.table({"a": pa.array([3, 4], pa.int8())})})
    write_match(root, "m1", {"events": old})
    write_match(root, "m2", {"events": pa.table({"a": pa.array([300, 5], pa.int16())})})
    return root


def make_scenario(kind, tmp_path, drift_collection):
    """A collection that grows, in the two ways the tests below cover."""
    if kind == "drift":
        return SimpleNamespace(
            full=drift_collection,
            first=FIRST,
            later=LATER,
            channels=CHANNELS,
            options={**SMALL},
        )
    return SimpleNamespace(
        full=events_collection(tmp_path / "events"),
        first=["m0", "m1"],
        later=["m2"],
        channels=["events"],
        options={"max_page_size_mb": 0.01, "read_threads": 0},
    )


def scenario_build(scenario, tome_root, ds_root, **kwargs):
    return make_curator(tome_root, ds_root).build_basic_tomes(
        scenario.channels, tome_name=BASIC, **{**scenario.options, **kwargs}
    )


# Resuming.


@pytest.mark.parametrize("reads", [3, 8, 13, 16])
def test_resume_equals_a_build_that_was_not_interrupted(
    tmp_path, drift_collection, monkeypatch, reads
):
    reference = build(make_curator(tmp_path / "reference", drift_collection))
    curator = make_curator(tmp_path / "tomes", drift_collection)
    interrupt_after(monkeypatch, reads)
    with pytest.raises(RuntimeError, match="interrupted"):
        build(curator)
    monkeypatch.undo()

    built = build(curator, resume=True)

    assert_same_collection(built, reference)
    assert not os.path.exists(staging(tmp_path / "tomes"))
    assert unlisted(tmp_path / "tomes") == []


def test_resume_reads_only_what_the_staged_pages_lack(
    tmp_path, drift_collection, monkeypatch
):
    curator = make_curator(tmp_path / "tomes", drift_collection)
    interrupt_after(monkeypatch, 13)
    with pytest.raises(RuntimeError, match="interrupted"):
        build(curator)
    monkeypatch.undo()
    staged = {
        channel: set(
            curator.get_loader(
                f"{builder.STAGING_FOLDER}/{HEADER}/{channel}"
            ).get_keyset()
        )
        for channel in CHANNELS
    }
    # Every channel file of a match its staged pages don't hold is read, and no other;
    # the header is built again, so each match's header row is read.
    folders = match_folders(drift_collection)
    have = {
        "player_death": ["m00", "m01", "m03", "m04", "m05"],
        "round_end": list(folders),
    }
    expected = 6 + sum(
        folders[name] + "/csds" not in staged[channel]
        for channel, names in have.items()
        for name in names
    )
    reads = count_reads(monkeypatch)

    build(curator, resume=True)

    assert len(reads) == expected
    assert 6 < expected < 17
    assert reads.count("header") == 6


def test_resume_is_off_by_default(tmp_path, drift_collection, monkeypatch):
    curator = make_curator(tmp_path / "tomes", drift_collection)
    interrupt_after(monkeypatch, 13)
    with pytest.raises(RuntimeError, match="interrupted"):
        build(curator)
    monkeypatch.undo()
    reads = count_reads(monkeypatch)

    build(curator)

    # Every file of every match: 6 headers, 5 player_death, 6 round_end.
    assert len(reads) == 17


@pytest.mark.parametrize("value,resumed", [("1", True), ("yes", True), ("0", False)])
def test_resume_can_be_set_in_the_environment(
    tmp_path, drift_collection, monkeypatch, value, resumed
):
    curator = make_curator(tmp_path / "tomes", drift_collection)
    interrupt_after(monkeypatch, 13)
    with pytest.raises(RuntimeError, match="interrupted"):
        build(curator)
    monkeypatch.undo()
    monkeypatch.setenv("PURESKILLGG_TOME_RESUME", value)
    reads = count_reads(monkeypatch)

    build(curator)

    assert (len(reads) < 17) is resumed


def test_resume_from_the_environment_must_be_a_flag(
    tmp_path, drift_collection, monkeypatch
):
    monkeypatch.setenv("PURESKILLGG_TOME_RESUME", "sometimes")

    with pytest.raises(ValueError, match="PURESKILLGG_TOME_RESUME"):
        build(make_curator(tmp_path / "tomes", drift_collection))


def test_resume_with_nothing_staged_is_a_build(tmp_path, drift_collection):
    reference = build(make_curator(tmp_path / "reference", drift_collection))

    built = build(make_curator(tmp_path / "tomes", drift_collection), resume=True)

    assert_same_collection(built, reference)


@pytest.mark.parametrize("old_value", [100, 200])
def test_resume_keeps_what_the_narrowing_needs_from_the_matches_it_does_not_read(
    tmp_path, monkeypatch, old_value
):
    # m0 is int8, m1 an old int64 column and m2 int16: the tome is int16 on every
    # page. The first page holds m0 and m1, and is narrowed to int8 (100) or kept
    # wide (200) while it is built; the build stops after it. Nothing on that
    # page says that m1 was int64, which is what makes the tome int16 and
    # not int8 (a mix of int8 and int16 alone is left as it is).
    root = events_collection(tmp_path / "ds")
    old = pd.DataFrame({"a": pd.array([old_value] + [1] * 999, dtype="Int64"), "n": 1})
    old.to_parquet(os.path.join(str(root), "csds/2022/05/15/m1/events"))
    options = {"max_page_size_mb": 0.01, "read_threads": 0}
    reference = make_curator(tmp_path / "reference", root).build_basic_tomes(
        ["events"], **options
    )
    curator = make_curator(tmp_path / "tomes", root)
    interrupt_after(monkeypatch, 4)
    with pytest.raises(RuntimeError, match="interrupted"):
        curator.build_basic_tomes(["events"], **options)
    monkeypatch.undo()
    staged = curator.get_loader(f"{builder.STAGING_FOLDER}/{HEADER}/events")
    assert len(staged.manifest["pages"]) == 1

    built = curator.build_basic_tomes(["events"], resume=True, **options)

    loader = built.tomes["events"]
    assert page_types(loader, "a") == {pa.int16()}
    assert_same_tome(loader, reference.tomes["events"])
    assert loader.get_dataframe()["a"].dtype == "Int16"


def test_resume_starts_a_channel_over_when_other_columns_are_read(
    tmp_path, drift_collection, monkeypatch
):
    log = RecordingLogger()
    columns = ["winner", "round"]
    reference = build(
        make_curator(tmp_path / "reference", drift_collection),
        [{"channel": "round_end", "columns": ["winner"]}],
    )
    curator = make_curator(tmp_path / "tomes", drift_collection, log=log)
    interrupt_after(monkeypatch, 9)
    with pytest.raises(RuntimeError, match="interrupted"):
        build(curator, [{"channel": "round_end", "columns": columns}])
    monkeypatch.undo()
    reads = count_reads(monkeypatch)

    built = build(
        curator, [{"channel": "round_end", "columns": ["winner"]}], resume=True
    )

    assert_same_tome(built.tomes["round_end"], reference.tomes["round_end"])
    assert reads.count("round_end") == 6
    assert log.named(
        "Staged tome can't be continued (built for other columns or "
        "another tome, or by dsdk 4.1.1): starting over"
    )


def test_resume_reads_a_match_again_when_its_staged_page_is_damaged(
    tmp_path, drift_collection, monkeypatch
):
    log = RecordingLogger()
    reference = build(make_curator(tmp_path / "reference", drift_collection))
    curator = make_curator(tmp_path / "tomes", drift_collection, log=log)
    interrupt_after(monkeypatch, 16)
    with pytest.raises(RuntimeError, match="interrupted"):
        build(curator)
    monkeypatch.undo()
    staged = curator.get_loader(f"{builder.STAGING_FOLDER}/{HEADER}/player_death")
    pages = staged.manifest["pages"]
    assert len(pages) >= 3
    # A page cut short, as a crash leaves it, and a missing keyset after it.
    damaged = get_page_path_fs(str(tmp_path / "tomes"), "dataframe", pages[1])
    with open(damaged, "r+b") as file:
        file.truncate(os.path.getsize(damaged) // 2)
    os.remove(get_page_path_fs(str(tmp_path / "tomes"), "keyset", pages[2]))

    built = build(curator, resume=True)

    assert_same_collection(built, reference)
    assert log.named("Staged pages missing or unreadable: reading their matches again")


def test_resume_lists_the_staged_matches_first_when_the_order_changed(
    tmp_path, drift_collection, monkeypatch
):
    # A collection that grew since the build stopped: the new matches sort among
    # the staged ones. The tomes hold the same matches and rows.
    log = RecordingLogger()
    keys = keys_of(drift_collection, FIRST + LATER)
    reversed_keys = list(reversed(keys))
    reference = build(
        make_curator(tmp_path / "reference", drift_collection), keys=reversed_keys
    )
    curator = make_curator(tmp_path / "tomes", drift_collection, log=log)
    interrupt_after(monkeypatch, 13)
    with pytest.raises(RuntimeError, match="interrupted"):
        build(curator, keys=keys)
    monkeypatch.undo()

    built = build(curator, keys=reversed_keys, resume=True)

    assert built.header.get_keyset() == reversed_keys
    assert log.named(
        "Resumed tomes list the matches they staged first, then the others: "
        "not in the order of the header"
    )
    for channel, loader in built.tomes.items():
        old = reference.tomes[channel]
        assert sorted(loader.get_keyset()) == sorted(old.get_keyset())
        assert_same_rows(loader, old, channel)
        assert set(loader.header.get_keyset()) == set(old.header.get_keyset())


def test_resume_refuses_a_match_that_is_gone(tmp_path, drift_collection, monkeypatch):
    curator = make_curator(tmp_path / "tomes", drift_collection)
    interrupt_after(monkeypatch, 13)
    with pytest.raises(RuntimeError, match="interrupted"):
        build(curator)
    monkeypatch.undo()
    shutil.rmtree(
        os.path.join(
            str(drift_collection), *match_folders(drift_collection)["m00"].split("/")
        )
    )

    with pytest.raises(ValueError, match="Can't resume.*not found"):
        build(curator, resume=True)


def test_resume_after_a_stop_while_publishing(tmp_path, drift_collection, monkeypatch):
    # The header is complete; the first channel is published; the second one
    # stopped after its pages were moved out of the staging folder.
    log = RecordingLogger()
    reference = build(make_curator(tmp_path / "reference", drift_collection))
    curator = make_curator(tmp_path / "tomes", drift_collection, log=log)
    real = builder._Build._copy_header  # pylint: disable=protected-access
    calls = {"n": 0}

    def copy_header(self, name, keys):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("stopped")
        real(self, name, keys)

    monkeypatch.setattr(builder._Build, "_copy_header", copy_header)
    with pytest.raises(RuntimeError, match="stopped"):
        build(curator)
    monkeypatch.undo()
    assert curator.get_header_loader().is_complete
    reads = count_reads(monkeypatch)

    built = build(curator, resume=True)

    assert_same_collection(built, reference)
    # No header row is read again, nor the channel that was published.
    assert "header" not in reads
    assert "player_death" not in reads
    assert unlisted(tmp_path / "tomes") == []


# Extending.


def test_extend_equals_a_build_of_all_the_matches(
    tmp_path, arrival, drift_collection, monkeypatch
):
    reference = build(
        make_curator(tmp_path / "reference", drift_collection), tome_name=BASIC
    )
    curator = make_curator(tmp_path / "tomes", arrival.root)
    first = build(curator, tome_name=BASIC)
    # The loaders cache their manifests: keep what the first build wrote.
    first_manifests = {
        c: json.loads(json.dumps(l.manifest)) for c, l in first.tomes.items()
    }
    first_header_id = first.header.manifest["id"]
    before = snapshot(tmp_path / "tomes")
    arrival.arrive()
    reads = count_reads(monkeypatch)

    extended = build(curator, tome_name=BASIC, behavior_if_complete="extend")

    assert_same_collection(extended, reference)
    # Only the new matches are read: their header rows and channel files.
    assert sorted(reads) == ["header"] * 3 + ["player_death"] * 3 + ["round_end"] * 3
    after = snapshot(tmp_path / "tomes")
    for channel, loader in extended.tomes.items():
        old_pages = first_manifests[channel]["pages"]
        pages = loader.manifest["pages"]
        # The old pages are the same files, then the new pages follow.
        assert pages[: len(old_pages)] == old_pages
        assert len(pages) > len(old_pages)
        assert [page["number"] for page in pages] == list(range(len(pages)))
        assert loader.manifest["id"] == first_manifests[channel]["id"]
        assert loader.manifest["isComplete"] is True
        for page in old_pages:
            for subtype in ("dataframe", "keyset"):
                relative = os.path.relpath(
                    get_page_path_fs(str(tmp_path / "tomes"), subtype, page),
                    str(tmp_path / "tomes"),
                )
                assert after[relative] == before[relative]
    assert extended.header.manifest["id"] == first_header_id
    # The header stays one page, number 0, as tools that read keyset_00000 expect.
    assert [p["number"] for p in extended.header.manifest["pages"]] == [0]
    header_folder = os.path.join(str(tmp_path / "tomes"), "tome", DS_TYPE, HEADER)
    assert sorted(os.listdir(header_folder)) == [
        "dataframe_00000",
        "keyset_00000",
        "tome",
    ]
    assert unlisted(tmp_path / "tomes") == []
    assert not os.path.exists(staging(tmp_path / "tomes"))


def test_extend_twice_equals_one_build(tmp_path, arrival, drift_collection):
    reference = build(
        make_curator(tmp_path / "reference", drift_collection), tome_name=BASIC
    )
    curator = make_curator(tmp_path / "tomes", arrival.root)
    build(curator, tome_name=BASIC)
    arrival.arrive(["m03", "m04"])
    build(curator, tome_name=BASIC, behavior_if_complete="extend")
    arrival.arrive(["m05"])

    extended = build(curator, tome_name=BASIC, behavior_if_complete="extend")

    assert_same_collection(extended, reference)
    assert unlisted(tmp_path / "tomes") == []


def test_extend_without_new_matches_changes_and_reads_nothing(
    tmp_path, arrival, monkeypatch
):
    arrival.arrive()
    curator = make_curator(tmp_path / "tomes", arrival.root)
    first = build(curator, tome_name=BASIC)
    before = snapshot(tmp_path / "tomes")
    reads = count_reads(monkeypatch)

    again = build(curator, tome_name=BASIC, behavior_if_complete="extend")

    assert not reads
    assert snapshot(tmp_path / "tomes") == before
    assert set(again.tomes) == set(first.tomes)
    assert again.dates == first.dates
    assert_same_collection(again, first)


def test_extend_removes_what_an_earlier_build_left_unlisted(tmp_path, arrival):
    arrival.arrive()
    curator = make_curator(tmp_path / "tomes", arrival.root)
    first = build(curator, tome_name=BASIC)
    folder = os.path.join(str(tmp_path / "tomes"), "tome", DS_TYPE, "basic_round_end")
    strays = [
        os.path.join(folder, "dataframe_00099"),
        os.path.join(folder, "keyset_00099"),
        os.path.join(folder, "tome.4242.tmp"),
        os.path.join(folder, "rewrite_dataframe_00003"),
        os.path.join(folder, "rewrite_keyset_00003"),
        os.path.join(folder, "header", "dataframe_00007"),
    ]
    for path in strays:
        with open(path, "wb") as file:
            file.write(b"left by a build that stopped")

    build(curator, tome_name=BASIC, behavior_if_complete="extend")

    assert not any(os.path.exists(path) for path in strays)
    assert_same_collection(
        build(curator, tome_name=BASIC, behavior_if_complete="extend"), first
    )


def test_extend_adds_a_match_given_twice_once(tmp_path, arrival, drift_collection):
    reference = build(
        make_curator(tmp_path / "reference", drift_collection), tome_name=BASIC
    )
    curator = make_curator(tmp_path / "tomes", arrival.root)
    build(curator, tome_name=BASIC)
    arrival.arrive()
    everything = keys_of(arrival.full, FIRST + LATER)

    # The keys the tomes hold are left out, and the new ones are added once.
    extended = build(
        curator,
        tome_name=BASIC,
        behavior_if_complete="extend",
        keys=[*everything, *everything[3:], everything[0]],
    )

    assert_same_collection(extended, reference)


def test_extend_adds_a_match_declared_by_two_manifests_once(
    tmp_path, arrival, drift_collection
):
    reference = build(
        make_curator(tmp_path / "reference", drift_collection), tome_name=BASIC
    )
    curator = make_curator(tmp_path / "tomes", arrival.root)
    build(curator, tome_name=BASIC)
    arrival.arrive()
    # A copy of a new match in another folder, whose manifest declares the same key.
    folders = match_folders(arrival.root)
    original = os.path.join(str(arrival.root), *folders["m03"].split("/"))
    copied = original + "copy"
    shutil.copytree(original, copied)
    manifest_path = os.path.join(copied, DS_TYPE)
    with gzip.open(manifest_path, "rb") as file:
        manifest = json.loads(file.read())
    manifest["key"] = folders["m03"] + "/" + DS_TYPE
    with gzip.open(manifest_path, "wb") as file:
        file.write(json.dumps(manifest).encode("utf8"))
    log = RecordingLogger()

    extended = build(
        make_curator(tmp_path / "tomes", arrival.root, log=log),
        tome_name=BASIC,
        behavior_if_complete="extend",
    )

    assert_same_collection(extended, reference)
    assert log.named("Duplicate match keys dropped") == [{"count": 1}]


def test_extend_takes_only_the_keys_it_is_given(tmp_path, arrival, drift_collection):
    curator = make_curator(tmp_path / "tomes", arrival.root)
    build(curator, tome_name=BASIC)
    arrival.arrive()
    keys = keys_of(arrival.full, FIRST + ["m03"])
    reference = build(
        make_curator(tmp_path / "reference", drift_collection),
        tome_name=BASIC,
        keys=keys,
    )

    extended = build(curator, tome_name=BASIC, behavior_if_complete="extend", keys=keys)

    assert_same_collection(extended, reference)
    assert len(extended.header.get_keyset()) == 4


def test_extend_appends_matches_that_sort_before_the_old_ones(
    tmp_path, drift_collection
):
    reference = build(
        make_curator(tmp_path / "reference", drift_collection), tome_name=BASIC
    )
    growing = tmp_path / "growing"
    copy_matches(drift_collection, growing, LATER)
    curator = make_curator(tmp_path / "tomes", growing)
    build(curator, tome_name=BASIC)
    copy_matches(drift_collection, growing, FIRST)

    extended = build(curator, tome_name=BASIC, behavior_if_complete="extend")

    # The new matches follow the old ones, so the order isn't the sorted one,
    # but every tome holds the same matches and rows.
    keys = extended.header.get_keyset()
    assert keys == keys_of(growing, LATER + FIRST)
    assert sorted(keys) == reference.header.get_keyset()
    for channel, loader in extended.tomes.items():
        old = reference.tomes[channel]
        assert sorted(loader.get_keyset()) == sorted(old.get_keyset())
        assert len(frame(loader)) == len(frame(old))
        assert_same_rows(loader, old, channel)
        assert set(loader.header.get_keyset()) == set(old.header.get_keyset())


def test_extend_builds_a_channel_that_has_no_tome_yet(
    tmp_path, arrival, drift_collection
):
    reference = build(
        make_curator(tmp_path / "reference", drift_collection), tome_name=BASIC
    )
    curator = make_curator(tmp_path / "tomes", arrival.root)
    first = build(curator, ["round_end"], tome_name=BASIC)
    arrival.arrive()

    extended = build(curator, tome_name=BASIC, behavior_if_complete="extend")

    # round_end is extended; player_death is built over the old matches too.
    assert_same_collection(extended, reference)
    assert (
        extended.tomes["round_end"].manifest["id"]
        == first.tomes["round_end"].manifest["id"]
    )


def test_extend_publishes_a_new_channel_before_the_header(
    tmp_path, arrival, monkeypatch
):
    # The header lists the matches, so it is written last: a stop after the header
    # must never leave a channel that the header's matches need still unpublished.
    curator = make_curator(tmp_path / "tomes", arrival.root)
    build(curator, ["round_end"], tome_name=BASIC)
    arrival.arrive()
    order = []
    real_publish, real_header = builder._Build._publish, builder._Build._commit_header

    def publish(self, channel, scribe, name, staged_header=None):
        order.append(("channel", channel))
        return real_publish(self, channel, scribe, name, staged_header)

    def commit_header(self, header, state):
        order.append(("header", None))
        return real_header(self, header, state)

    monkeypatch.setattr(builder._Build, "_publish", publish)
    monkeypatch.setattr(builder._Build, "_commit_header", commit_header)
    build(curator, tome_name=BASIC, behavior_if_complete="extend")

    kinds = [kind for kind, _ in order]
    assert ("channel", "player_death") in order
    assert kinds.index("channel") < kinds.index("header")


def test_extend_stopped_between_the_header_files_is_repaired_by_the_next_run(
    tmp_path, arrival, drift_collection, monkeypatch
):
    # The header's page 0 is replaced file by file (rows, then keyset). A stop
    # between the two must leave a collection that the next extension repairs.
    reference = build(
        make_curator(tmp_path / "reference", drift_collection), tome_name=BASIC
    )
    curator = make_curator(tmp_path / "tomes", arrival.root)
    build(curator, tome_name=BASIC)
    arrival.arrive()
    real_replace = builder.replace_file

    def stop_before_the_keyset(source, destination, *args, **kwargs):
        if os.path.basename(str(destination)) == "keyset_00000" and "header" in str(
            destination
        ):
            raise RuntimeError("stopped between the header files")
        return real_replace(source, destination, *args, **kwargs)

    monkeypatch.setattr(builder, "replace_file", stop_before_the_keyset)
    with pytest.raises(RuntimeError, match="stopped between the header files"):
        build(curator, tome_name=BASIC, behavior_if_complete="extend")
    monkeypatch.setattr(builder, "replace_file", real_replace)

    repaired = build(curator, tome_name=BASIC, behavior_if_complete="extend")
    assert_same_collection(repaired, reference)


def test_extend_stopped_after_the_channels_and_before_the_header_is_repaired(
    tmp_path, arrival, drift_collection, monkeypatch
):
    # The channels are committed first and the header last. A stop in between
    # leaves the header without the new matches (the list is the header's);
    # the next extension adds them to the header and not to the channels again.
    reference = build(
        make_curator(tmp_path / "reference", drift_collection), tome_name=BASIC
    )
    curator = make_curator(tmp_path / "tomes", arrival.root)
    build(curator, tome_name=BASIC)
    arrival.arrive()
    real_commit_header = builder._Build._commit_header

    def stop(self, header, state):
        raise RuntimeError("stopped before the header")

    monkeypatch.setattr(builder._Build, "_commit_header", stop)
    with pytest.raises(RuntimeError, match="stopped before the header"):
        build(curator, tome_name=BASIC, behavior_if_complete="extend")
    monkeypatch.setattr(builder._Build, "_commit_header", real_commit_header)

    repaired = build(curator, tome_name=BASIC, behavior_if_complete="extend")
    assert_same_collection(repaired, reference)


def test_extend_widens_what_the_old_pages_hold(tmp_path):
    # The first pages are int8; the new match is int16, so every page becomes
    # int16 as in a build of all the matches. The old page is written again,
    # whole, in place.
    log = RecordingLogger()
    full = events_collection(tmp_path / "ds")
    growing = tmp_path / "growing"
    copy_matches(full, growing, ["m0", "m1"])
    options = {"max_page_size_mb": 0.01, "read_threads": 0, "tome_name": BASIC}
    reference = make_curator(tmp_path / "reference", full).build_basic_tomes(
        ["events"], **options
    )
    curator = make_curator(tmp_path / "tomes", growing, log=log)
    first = curator.build_basic_tomes(["events"], **options)
    assert page_types(first.tomes["events"], "a") == {pa.int8()}
    old_pages = first.tomes["events"].manifest["pages"]
    copy_matches(full, growing, ["m2"])

    extended = curator.build_basic_tomes(
        ["events"], behavior_if_complete="extend", **options
    )

    loader = extended.tomes["events"]
    assert page_types(loader, "a") == {pa.int16()}
    assert loader.manifest["pages"][:1] == old_pages
    assert [kw["page_number"] for kw in log.named("Page narrowed")] == [0]
    assert loader.get_dataframe()["a"].dtype == "Int16"
    assert_same_tome(loader, reference.tomes["events"])
    assert unlisted(tmp_path / "tomes") == []


def test_extend_narrows_the_new_matches_to_the_types_of_the_old_ones(tmp_path):
    full = tmp_path / "ds"
    old = pd.DataFrame({"a": pd.array([100] + [1] * 999, dtype="Int64"), "n": 1})
    write_match(full, "m0", {"events": pa.table({"a": pa.array([3, 4], pa.int8())})})
    write_match(full, "m1", {"events": old})
    growing = tmp_path / "growing"
    copy_matches(full, growing, ["m0"])
    options = {"max_page_size_mb": 0.01, "read_threads": 0, "tome_name": BASIC}
    reference = make_curator(tmp_path / "reference", full).build_basic_tomes(
        ["events"], **options
    )
    curator = make_curator(tmp_path / "tomes", growing)
    curator.build_basic_tomes(["events"], **options)
    copy_matches(full, growing, ["m1"])

    extended = curator.build_basic_tomes(
        ["events"], behavior_if_complete="extend", **options
    )

    assert page_types(extended.tomes["events"], "a") == {pa.int8()}
    assert_same_tome(extended.tomes["events"], reference.tomes["events"])


def test_extend_that_a_value_does_not_fit_changes_nothing(tmp_path):
    # The new match is int64 and the old ones int8, so it is narrowed, and 1000
    # doesn't fit: the build stops before it changes a file.
    full = tmp_path / "ds"
    write_match(full, "m0", {"events": pa.table({"a": pa.array([3, 4], pa.int8())})})
    write_match(
        full, "m1", {"events": pd.DataFrame({"a": pd.array([1000, 1], dtype="Int64")})}
    )
    growing = tmp_path / "growing"
    copy_matches(full, growing, ["m0"])
    options = {"max_page_size_mb": 0.01, "read_threads": 0, "tome_name": BASIC}
    curator = make_curator(tmp_path / "tomes", growing)
    curator.build_basic_tomes(["events"], **options)
    before = snapshot(tmp_path / "tomes")
    copy_matches(full, growing, ["m1"])

    with pytest.raises(NarrowingError, match="1000"):
        curator.build_basic_tomes(["events"], behavior_if_complete="extend", **options)

    assert snapshot(tmp_path / "tomes") == before


def test_extend_a_tome_without_signatures_in_its_manifest(
    tmp_path, arrival, drift_collection
):
    # Tomes built by dsdk 4.1.1 or earlier have no signatures field.
    reference = build(
        make_curator(tmp_path / "reference", drift_collection), tome_name=BASIC
    )
    curator = make_curator(tmp_path / "tomes", arrival.root)
    first = build(curator, tome_name=BASIC)
    for loader in [first.header, *first.tomes.values()]:
        path = os.path.join(str(tmp_path / "tomes"), *loader.manifest["key"].split("/"))
        manifest = dict(loader.manifest)
        assert manifest.pop("signatures")
        with open(path, "w", encoding="utf-8") as file:
            json.dump(manifest, file)
    arrival.arrive()

    extended = build(curator, tome_name=BASIC, behavior_if_complete="extend")

    assert_same_collection(extended, reference)
    assert all("signatures" in loader.manifest for loader in extended.tomes.values())


def test_extend_refuses_a_tome_with_a_page_missing(tmp_path, arrival):
    curator = make_curator(tmp_path / "tomes", arrival.root)
    first = build(curator, tome_name=BASIC)
    page = first.tomes["round_end"].manifest["pages"][0]
    os.remove(get_page_path_fs(str(tmp_path / "tomes"), "dataframe", page))
    arrival.arrive()

    with pytest.raises(Exception, match="can't be extended"):
        build(curator, tome_name=BASIC, behavior_if_complete="extend")


def test_extend_needs_a_name_without_dates(tmp_path, drift_collection):
    with pytest.raises(ValueError, match=r"without \{dates\}"):
        make_curator(tmp_path / "tomes", drift_collection).build_basic_tomes(
            CHANNELS, behavior_if_complete="extend"
        )


def test_extend_on_nothing_is_a_build(tmp_path, drift_collection):
    reference = build(
        make_curator(tmp_path / "reference", drift_collection), tome_name=BASIC
    )

    built = build(
        make_curator(tmp_path / "tomes", drift_collection),
        tome_name=BASIC,
        behavior_if_complete="extend",
    )

    assert_same_collection(built, reference)


def test_extend_can_be_set_in_the_environment(tmp_path, arrival, monkeypatch):
    curator = make_curator(tmp_path / "tomes", arrival.root)
    build(curator, tome_name=BASIC)
    arrival.arrive()
    monkeypatch.setenv("PURESKILLGG_TOME_BEHAVIOR_IF_COMPLETE", "extend")

    extended = build(curator, tome_name=BASIC)

    assert len(extended.header.get_keyset()) == 6


def test_keys_that_differ_name_the_option_that_adds_them(tmp_path, drift_collection):
    curator = make_curator(tmp_path / "tomes", drift_collection)
    keys = build(curator, ["round_end"]).header.get_keyset()

    with pytest.raises(ValueError, match="behavior_if_complete='extend'"):
        build(curator, ["round_end"], keys=keys[:2])


@pytest.mark.parametrize("read_threads", [0, 4])
def test_extend_gives_the_same_with_threads(
    tmp_path, arrival, drift_collection, read_threads
):
    reference = build(
        make_curator(tmp_path / "reference", drift_collection), tome_name=BASIC
    )
    curator = make_curator(tmp_path / "tomes", arrival.root)
    build(curator, tome_name=BASIC)
    arrival.arrive()

    extended = build(
        curator,
        tome_name=BASIC,
        behavior_if_complete="extend",
        read_threads=read_threads,
    )

    assert_same_collection(extended, reference)


# Stopping an extension.


def test_extend_stopped_while_reading_leaves_the_tomes_as_they_were(
    tmp_path, arrival, drift_collection, monkeypatch
):
    reference = build(
        make_curator(tmp_path / "reference", drift_collection), tome_name=BASIC
    )
    curator = make_curator(tmp_path / "tomes", arrival.root)
    build(curator, tome_name=BASIC)
    before = snapshot(tmp_path / "tomes")
    arrival.arrive()
    interrupt_after(monkeypatch, 5)
    with pytest.raises(RuntimeError, match="interrupted"):
        build(curator, tome_name=BASIC, behavior_if_complete="extend")
    monkeypatch.undo()

    assert snapshot(tmp_path / "tomes") == before
    extended = build(curator, tome_name=BASIC, behavior_if_complete="extend")

    assert_same_collection(extended, reference)


@pytest.mark.parametrize("reads", [4, 6, 8])
def test_extend_resumes_where_it_stopped(
    tmp_path, arrival, drift_collection, monkeypatch, reads
):
    reference = build(
        make_curator(tmp_path / "reference", drift_collection), tome_name=BASIC
    )
    curator = make_curator(tmp_path / "tomes", arrival.root)
    build(curator, tome_name=BASIC)
    arrival.arrive()
    interrupt_after(monkeypatch, reads)
    with pytest.raises(RuntimeError, match="interrupted"):
        build(curator, tome_name=BASIC, behavior_if_complete="extend")
    monkeypatch.undo()
    counted = count_reads(monkeypatch)

    extended = build(
        curator, tome_name=BASIC, behavior_if_complete="extend", resume=True
    )

    assert_same_collection(extended, reference)
    # The new matches have 3 header rows and 6 channel files in all.
    assert 3 <= len(counted) < 9
    assert unlisted(tmp_path / "tomes") == []


def crash_in_commit(monkeypatch, stop_at):
    """
    Make the ``stop_at``-th change to a file from the first change of a
    finished tome on raise (0 never does). Returns how many there were.
    """
    state = {"armed": False, "changes": 0}

    def change():
        if not state["armed"]:
            return
        state["changes"] += 1
        if state["changes"] == stop_at:
            raise RuntimeError("stopped")

    def arm(real):
        def armed(self, *args):
            state["armed"] = True
            return real(self, *args)

        return armed

    def before(real):
        def wrapped(*args, **kwargs):
            change()
            return real(*args, **kwargs)

        return wrapped

    monkeypatch.setattr(
        builder._Build,  # pylint: disable=protected-access
        "_commit_extension",
        arm(builder._Build._commit_extension),  # pylint: disable=protected-access
    )
    monkeypatch.setattr(
        TomeWriterFs, "write_manifest", before(TomeWriterFs.write_manifest)
    )
    monkeypatch.setattr(TomeWriterFs, "write_page", before(TomeWriterFs.write_page))
    monkeypatch.setattr(builder, "replace_file", before(builder.replace_file))
    monkeypatch.setattr(builder, "remove_orphans", before(builder.remove_orphans))
    return state


@pytest.mark.parametrize("kind", ["drift", "widen"])
def test_extend_stopped_at_any_step_of_the_commit_leaves_whole_tomes_and_is_completed(
    tmp_path, drift_collection, kind, monkeypatch
):
    scenario = make_scenario(kind, tmp_path, drift_collection)
    reference = scenario_build(scenario, tmp_path / "reference", scenario.full)
    growing = tmp_path / "growing"
    copy_matches(scenario.full, growing, scenario.first)
    scenario_build(scenario, tmp_path / "first", growing)
    copy_matches(scenario.full, growing, scenario.later)
    all_keys = set(keys_of(scenario.full, scenario.first + scenario.later))
    first_keys = set(keys_of(scenario.full, scenario.first))

    # How many changes there are, from a run that isn't stopped.
    shutil.copytree(tmp_path / "first", tmp_path / "count")
    counter = crash_in_commit(monkeypatch, 0)
    scenario_build(scenario, tmp_path / "count", growing, behavior_if_complete="extend")
    monkeypatch.undo()
    changes = counter["changes"]
    assert changes >= 8
    print(f"{kind}: the commit makes {changes} file changes; stopping at each")

    for stop_at in range(1, changes + 1):
        work = tmp_path / f"stopped_{stop_at}"
        shutil.copytree(tmp_path / "first", work)
        crash_in_commit(monkeypatch, stop_at)
        with pytest.raises(RuntimeError, match="stopped"):
            scenario_build(scenario, work, growing, behavior_if_complete="extend")
        monkeypatch.undo()

        # Every tome reads whole, whatever was stopped; the header lists the
        # old matches, or all of them once it is replaced.
        curator = make_curator(work, growing)
        header = curator.get_header_loader()
        assert header.manifest["isComplete"] is True
        assert set(header.get_keyset()) in (first_keys, all_keys)
        # The header's rows are replaced before its keyset, which lists the
        # matches, so the rows may be ahead of the keyset, never behind.
        assert len(header.get_keyset()) <= len(frame(header)) <= len(all_keys)
        for channel in scenario.channels:
            loader = curator.get_loader(f"basic_{channel}")
            assert loader.manifest["isComplete"] is True
            assert set(loader.get_keyset()) <= all_keys
            assert frame(loader)["match_key"].nunique() <= len(loader.get_keyset())
            # The header copy is written before the manifest that lists the new
            # matches, so it may be ahead of the tome, never behind.
            copy = set(loader.header.get_keyset())
            assert set(loader.get_keyset()) <= copy <= all_keys
            assert len(frame(loader.header)) == len(loader.header.get_keyset())

        completed = scenario_build(
            scenario, work, growing, behavior_if_complete="extend"
        )

        assert_same_collection(completed, reference)
        assert unlisted(work) == [], stop_at
        assert not os.path.exists(staging(work))
