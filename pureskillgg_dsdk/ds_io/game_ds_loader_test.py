# pylint: disable=missing-docstring
# pylint: disable=unused-import
# pylint: disable=unused-argument
# pylint: disable=no-self-use
# pylint: disable=protected-access

from glob import glob

import os
import pandas as pd
import pytest
from .game_ds_loader import GameDsLoader
from .reader_fs import DsReaderFs
from .normalize_instructions import normalize_instructions


def create_test_game_ds_loader():
    root = "fixtures"
    job_id = "0F9P4Bte2Z1GLiuOsryY"
    manifest_key = os.path.join(job_id, "csds")
    reader = DsReaderFs(root_path=root, manifest_key=manifest_key)
    loader = GameDsLoader(reader=reader)
    return loader


def test_manifest():
    loader = create_test_game_ds_loader()
    assert "channels" in loader.manifest
    assert isinstance(loader.metadata, dict)


def test_metadata():
    loader = create_test_game_ds_loader()
    assert isinstance(loader.metadata, dict)


def test_blank_instructions():
    loader = create_test_game_ds_loader()
    data = loader.get_channels()
    assert len(data.keys()) == len(loader.manifest["channels"])


def test_reading_channels_with_instructions():
    loader = create_test_game_ds_loader()
    data = loader.get_channels([{"channel": "round_end"}])
    assert "round_end" in data
    assert isinstance(data["round_end"], pd.DataFrame)


def test_single_channel():
    loader = create_test_game_ds_loader()
    df = loader.get_channel({"channel": "round_end"})
    assert isinstance(df, pd.DataFrame)


def test_channels_with_column():
    loader = create_test_game_ds_loader()
    data = loader.get_channels([{"channel": "round_end", "columns": ["tick"]}])
    assert len(data["round_end"].columns) == 1


def test_single_channel_with_column():
    loader = create_test_game_ds_loader()
    df = loader.get_channel({"channel": "round_end", "columns": ["tick"]})
    assert isinstance(df, pd.DataFrame)
    assert len(df.columns) == 1
