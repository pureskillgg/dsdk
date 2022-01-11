# pylint: disable=missing-docstring
# pylint: disable=unused-import
# pylint: disable=unused-argument
import os
import itertools
import pytest
import pandas as pd
from .curator import TomeCuratorFs


# pylint: disable=invalid-name
ds_collection_root_path = "fixtures"
ds_type = "csds"
default_header_name = "header_tome"
sub_header_name = "only_good_headers"
new_tome_name = "round_end"
continued_tome_name = "round_end_continued"
pd.DataFrame()


def create_curator_instance(tmp_path):
    return TomeCuratorFs(
        default_header_name=default_header_name,
        ds_type=ds_type,
        tome_collection_root_path=tmp_path,
        ds_collection_root_path=ds_collection_root_path,
    )


def create_header(curator):
    return curator.create_header_tome(path_depth=1)


def create_header_and_subheader(curator):
    create_header(curator)
    return curator.create_subheader_tome(
        sub_header_name,
        lambda df: df["key"] != "0F9P4Bte2Z1GLiuOsryY",
    )


def test_create_header_tome(tmp_path):
    tmp_path = str(tmp_path)
    curator = create_curator_instance(tmp_path)
    loader = create_header(curator)

    df = loader.get_dataframe()
    keyset = loader.get_keyset()
    manifest = loader.manifest

    assert len(df) == len(keyset)
    assert len(df) == 4
    assert isinstance(manifest, dict)


def test_create_subheader_tome(tmp_path):
    tmp_path = str(tmp_path)
    curator = create_curator_instance(tmp_path)
    loader = create_header_and_subheader(curator)

    df = loader.get_dataframe()
    keyset = loader.get_keyset()
    manifest = loader.manifest

    assert len(df) == len(keyset)
    assert len(df) == 3
    assert isinstance(manifest, dict)


def test_make_tome(tmp_path):
    tmp_path = str(tmp_path)
    curator = create_curator_instance(tmp_path)
    create_header_and_subheader(curator)

    tomer = curator.make_tome(
        new_tome_name,
        header_tome_name=sub_header_name,
        ds_reading_instructions=[{"channel": "round_end"}],
    )

    for data, _ in tomer.iterate():
        tomer.concat(data["round_end"])

    df = curator.get_dataframe(new_tome_name)
    keyset = curator.get_keyset(new_tome_name)
    assert isinstance(df, pd.DataFrame)
    assert isinstance(keyset, list)


def test_make_tome_with_options(tmp_path):
    new_tome_name_size_limited = "round_end_size_limited"
    new_tome_name_row_limited = "round_end_row_limited"
    curator = create_curator_instance(tmp_path)
    create_header_and_subheader(curator)
    tomer = curator.make_tome(
        new_tome_name_size_limited,
        header_tome_name=sub_header_name,
        ds_reading_instructions=[{"channel": "round_end"}],
        max_page_size_mb=0.00001,
        limit_check_frequency=1,
    )

    for data, _ in tomer.iterate():
        tomer.concat(data["round_end"])

    df = curator.get_dataframe(new_tome_name_size_limited)
    keyset = curator.get_keyset(new_tome_name_size_limited)
    assert isinstance(df, pd.DataFrame)
    assert isinstance(keyset, list)

    tomer = curator.make_tome(
        new_tome_name_row_limited,
        header_tome_name=sub_header_name,
        ds_reading_instructions=[{"channel": "round_end"}],
        max_page_row_count=1,
        limit_check_frequency=1,
    )

    for data, _ in tomer.iterate():
        tomer.concat(data["round_end"])

    df = curator.get_dataframe(new_tome_name_row_limited)
    keyset = curator.get_keyset(new_tome_name_row_limited)
    assert isinstance(df, pd.DataFrame)
    assert isinstance(keyset, list)


def continue_tomer_generator(
    curator,
    behavior_if_complete="pass",
    behavior_if_partial="continue",
):
    tomer = curator.make_tome(
        continued_tome_name,
        header_tome_name=sub_header_name,
        ds_reading_instructions=[{"channel": "round_end"}],
        max_page_row_count=1,
        limit_check_frequency=1,
        behavior_if_complete=behavior_if_complete,
        behavior_if_partial=behavior_if_partial,
    )
    return tomer


def create_partial_tome(curator):
    create_header_and_subheader(curator)
    tomer = continue_tomer_generator(curator)
    for data, _ in tomer.iterate():
        tomer.concat(data["round_end"])
        break


def create_complete_tome(curator):
    create_header_and_subheader(curator)
    tomer = continue_tomer_generator(curator)
    for data, _ in tomer.iterate():
        tomer.concat(data["round_end"])


def test_make_tome_breaking(tmp_path):
    curator = create_curator_instance(tmp_path)
    create_partial_tome(curator)
    keyset = curator.get_keyset(continued_tome_name)
    assert len(keyset) == 1
    assert curator.get_manifest(continued_tome_name)["isComplete"] is False


def test_make_tome_existing_behavior_pass(tmp_path):
    curator = create_curator_instance(tmp_path)
    create_partial_tome(curator)
    tome_id = curator.get_manifest(continued_tome_name)["id"]
    tomer = continue_tomer_generator(
        curator, continued_tome_name, behavior_if_partial="pass"
    )
    for data, _ in tomer.iterate():
        tomer.concat(data["round_end"])
    keyset = curator.get_keyset(continued_tome_name)
    assert len(keyset) == 1
    assert curator.get_manifest(continued_tome_name)["isComplete"] is False
    assert tome_id == curator.get_manifest(continued_tome_name)["id"]


def test_make_tome_existing_behavior_continue(tmp_path):
    curator = create_curator_instance(tmp_path)
    create_partial_tome(curator)
    tome_id = curator.get_manifest(continued_tome_name)["id"]
    tomer = continue_tomer_generator(
        curator, continued_tome_name, behavior_if_partial="continue"
    )
    for data, _ in tomer.iterate():
        tomer.concat(data["round_end"])
        break
    keyset = curator.get_keyset(continued_tome_name)
    assert len(keyset) == 2
    assert tome_id == curator.get_manifest(continued_tome_name)["id"]
    assert curator.get_manifest(continued_tome_name)["isComplete"] is False


def test_make_tome_existing_behavior_overwrite(tmp_path):
    curator = create_curator_instance(tmp_path)
    create_partial_tome(curator)
    tomer = continue_tomer_generator(
        curator, continued_tome_name, behavior_if_partial="overwrite"
    )
    tome_id = curator.get_manifest(continued_tome_name)["id"]
    for data, _ in tomer.iterate():
        tomer.concat(data["round_end"])
    assert tome_id != curator.get_manifest(continued_tome_name)["id"]
    assert curator.get_manifest(continued_tome_name)["isComplete"] is True


def test_make_tome_complete_behavior_continue(tmp_path):
    curator = create_curator_instance(tmp_path)
    create_complete_tome(curator)
    tome_id = curator.get_manifest(continued_tome_name)["id"]
    tomer = continue_tomer_generator(curator, behavior_if_complete="continue")
    for data, _ in tomer.iterate():
        tomer.concat(data["round_end"])
    assert tome_id == curator.get_manifest(continued_tome_name)["id"]


def test_make_tome_complete_behavior_overwrite(tmp_path):
    curator = create_curator_instance(tmp_path)
    create_complete_tome(curator)
    tome_id = curator.get_manifest(continued_tome_name)["id"]
    tomer = continue_tomer_generator(curator, behavior_if_complete="overwrite")
    for data, _ in tomer.iterate():
        tomer.concat(data["round_end"])
    assert tome_id != curator.get_manifest(continued_tome_name)["id"]
