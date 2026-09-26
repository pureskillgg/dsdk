# pylint: disable=missing-docstring,protected-access

import pandas as pd

from .maker import TomeMaker


class StubLoader:
    def __init__(self, keyset, *, header=None):
        self._keyset = keyset
        self.header = header
        self.exists = True
        self.manifest = {"isComplete": False}

    def get_dataframe(self):
        return pd.DataFrame({"key": self._keyset})

    def get_keyset(self):
        return list(self._keyset)


class StubScribe:
    tome_name = "stub"

    def __init__(self):
        self.manifest_data = None

    def set_manifest_data(self, data):
        self.manifest_data = data


def test_continue_tome_keeps_header_order():
    header_keys = [f"csds/2022/01/01/match-{i:03d}/csds" for i in range(50)]
    done_keys = header_keys[::3]
    header = StubLoader(header_keys)
    existing_tome = StubLoader(done_keys, header=header)
    maker = TomeMaker(
        header_loader=header,
        scribe=StubScribe(),
        ds_reading_instructions=None,
        ds_type="csds",
        tome_loader=existing_tome,
        header_copier=None,
        ds_collection_root_path="unused",
        behavior_if_partial="continue",
    )

    maker._load()

    assert maker.keyset == [key for key in header_keys if key not in done_keys]
