# pylint: disable=missing-docstring
# pylint: disable=unused-import
# pylint: disable=unused-argument
import os
import pytest
import pandas as pd
from .curator import TomeCuratorFs


# pylint: disable=invalid-name
tmp_dir = os.path.join("tmp")
ds_collection_root_path = "fixtures"
ds_type = "csds"
default_header_name = "header_tome"
sub_header_name = "only_good_headers"
new_tome_name = "round_end"


@pytest.fixture(autouse=True)
def run_before_and_after_tests(tmpdir):
    if not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)
    yield


def create_instance():
    return TomeCuratorFs(
        default_header_name=default_header_name,
        ds_type=ds_type,
        tome_collection_root_path=tmp_dir,
        ds_collection_root_path=ds_collection_root_path,
    )


def test_create_header_tome():
    curator = create_instance()
    loader = curator.create_header_tome(path_depth=1)

    df = loader.get_dataframe()
    keyset = loader.get_keyset()
    manifest = loader.manifest

    assert len(df) == len(keyset)
    assert len(df) == 4
    assert isinstance(manifest, dict)


def test_create_subheader_tome():
    curator = create_instance()
    loader = curator.create_subheader_tome(
        sub_header_name,
        lambda df: df["key"] != "0F9P4Bte2Z1GLiuOsryY",
    )

    df = loader.get_dataframe()
    keyset = loader.get_keyset()
    manifest = loader.manifest

    assert len(df) == len(keyset)
    assert len(df) == 3
    assert isinstance(manifest, dict)

    df = curator.get_dataframe(sub_header_name)
    keyset = curator.get_keyset(sub_header_name)
    assert len(df) == len(keyset)
    assert len(df) == 3


def test_new_tome():
    curator = create_instance()
    tomer = curator.new_tome(
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


def test_new_tome_with_options():
    new_tome_name_size_limited = "round_end_size_limited"
    new_tome_name_row_limited = "round_end_row_limited"
    curator = create_instance()
    tomer = curator.new_tome(
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

    tomer = curator.new_tome(
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
