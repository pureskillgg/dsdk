# pylint: disable=missing-docstring

import pytest

from .header_tome import create_header_tome_from_fs, get_manifest_key_paths_from_glob

DS_COLLECTION_ROOT_PATH = "fixtures"


def test_manifest_key_paths_are_sorted():
    key_paths = get_manifest_key_paths_from_glob(DS_COLLECTION_ROOT_PATH, "csds")

    assert isinstance(key_paths, list)
    assert len(key_paths) == 3
    assert key_paths == sorted(key_paths)


def test_create_header_tome_with_default_name(tmp_path):
    with pytest.warns(UserWarning, match="Header name of header"):
        loader = create_header_tome_from_fs(
            tome_collection_root_path=str(tmp_path),
            ds_collection_root_path=DS_COLLECTION_ROOT_PATH,
        )

    assert loader.manifest["tome"] == "header"
    keyset = loader.get_keyset()
    assert len(keyset) == 3
    assert keyset == sorted(keyset)
