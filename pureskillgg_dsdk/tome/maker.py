import time
import os
import structlog
import pandas as pd

from ..ds_io import DsReaderFs, GameDsLoader
from .constants import filter_ds_reader_logs


class TomeMaker:
    def __init__(
        self,
        *,
        header_loader,
        scribe,
        ds_reading_instructions,
        ds_type,
        tome_loader,
        copy_header,
        behavior_if_complete="pass",
        behavior_if_partial="continue",
        print_status_frequency=100,
        reader_class=DsReaderFs,
        log: object = None,
    ):
        self.is_started = False
        self.is_finished = False
        self.keyset = None
        self._header_dataframe = None
        self._log = log if log is not None else structlog.get_logger()
        self._scribe = scribe
        self._ds_type = ds_type
        self._ds_reading_instructions = ds_reading_instructions
        self._header_loader = header_loader
        self._existing_tome_loader = tome_loader
        self._copy_header_func = copy_header
        self._if_complete = behavior_if_complete
        self._if_partial = behavior_if_partial
        self._print_status_frequency = print_status_frequency
        self._reader_class = reader_class
        self._current_key = None
        self._loaded = False
        self._key_to_path_hashmap = {}

    def iterate(self):
        """Get data for the next key"""
        self._load()
        if len(self.keyset) == 0:
            return
        self._scribe.start()
        self._log = structlog.wrap_logger(self._log, processors=[filter_ds_reader_logs])

        start_time = time.time()
        key_counter = 0
        for key in self.keyset:
            self._current_key = key
            data = self._get_ds_channels_from_fs(key)
            yield data, key
            self._log_status(key_counter, start_time)
            key_counter += 1

        self._finish()

    def concat(self, df: pd.DataFrame):
        """Append a dataframe to tome dataset"""
        self._scribe.concat(df, self._current_key)

    def _finish(self):
        self._scribe.finish()

    def _log_status(self, key_counter: int, start_time: float):
        if key_counter % self._print_status_frequency == 0 and key_counter > 0:
            n_done = key_counter
            n_total = len(self.keyset)
            fraction_complete = n_done / n_total
            seconds_elapsed = time.time() - start_time
            time_remaining = (seconds_elapsed / fraction_complete) - seconds_elapsed
            meta = {
                "page_count": self._scribe.page_counter,
                "page_row_count": self._scribe.page_row_count,
                "page_size_mb": self._scribe.page_size_mb,
                "percent_complete": round(100 * fraction_complete, 3),
                "keys_complete": n_done,
                "minutes_elapsed": int(seconds_elapsed / 60),
                "minutes_remaining": int(time_remaining / 60),
                "hours_elapsed": int(seconds_elapsed / 60 / 60),
                "hours_remaining": int(time_remaining / 60 / 60),
            }
            self._log.info("Tome Maker Update:", **meta)

    def _get_ds_channels_from_fs(self, key):
        path_to_ds = self._key_to_path(key)
        root_path = (os.sep).join(path_to_ds.split(os.path.sep)[:-1])
        ds_key = path_to_ds.split(os.path.sep)[-1]
        ds_reader = self._reader_class(
            root_path=root_path,
            manifest_key=os.path.join(ds_key, self._ds_type),
            log=self._log,
        )
        ds_loader = GameDsLoader(reader=ds_reader, log=self._log)
        data = ds_loader.get_channels(self._ds_reading_instructions)
        return data

    def _load_actions(self):
        def continue_tome():
            self._header_dataframe = self._existing_tome_loader.header.get_dataframe()
            existing_keyset = self._existing_tome_loader.get_keyset()
            target_keyset = self._existing_tome_loader.header.get_keyset()
            self.keyset = list(set(target_keyset) - set(existing_keyset))
            self._scribe.set_manifest_data(self._existing_tome_loader.manifest)

        def overwrite():
            self._header_dataframe = self._header_loader.get_dataframe()
            self.keyset = self._header_loader.get_keyset()
            self._copy_header()

        def start_new():
            overwrite()

        def passthrough():
            self.keyset = []

        def fail():
            raise Exception(f"Tome already exists {self._scribe.tome_name}")

        return continue_tome, overwrite, start_new, passthrough, fail

    def _load(self):
        if self._loaded:
            return

        self._header_dataframe = self._header_loader.get_dataframe()

        continue_tome, overwrite, start_new, passthrough, fail = self._load_actions()

        exists = self._existing_tome_loader.exists
        if not exists:
            start_new()
        else:
            is_complete = self._existing_tome_loader.manifest["isComplete"]
            action = {
                "continue": continue_tome,
                "overwrite": overwrite,
                "pass": passthrough,
                "fail": fail,
            }[self._if_complete if is_complete else self._if_partial]
            if is_complete and self._if_complete == "continue":
                action = passthrough
            action()

        self._key_to_path_hashmap = self._header_dataframe.set_index("key").to_dict()[
            "ds_path"
        ]
        self._loaded = True

    def _copy_header(self):
        self._copy_header_func(self._header_loader, self._scribe, self._log)

    def _key_to_path(self, key: str) -> str:
        return self._key_to_path_hashmap[key]
