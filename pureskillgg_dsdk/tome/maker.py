import time
import os
import structlog
import pandas as pd

from ..ds_io import DsReaderFs, GameDsLoader


class TomeMaker:
    def __init__(
        self,
        *,
        header_loader,
        scribe,
        ds_reader_instructions,
        ds_type,
        print_status_frequency=100,
        reader_class=DsReaderFs,
        log: object = None
    ):
        self.is_started = False
        self.is_finished = False
        self.keyset = None
        self._log = log if log is not None else structlog.get_logger()
        self._scribe = scribe
        self._ds_type = ds_type
        self._ds_reader_instructions = ds_reader_instructions
        self._header_loader = header_loader
        self._print_status_frequency = print_status_frequency
        self._reader_class = reader_class
        self._current_key = None
        self._loaded = False
        self._key_to_path_hashmap = {}

    def iterate(self):
        """Get data for the next key"""
        self._load()
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
            time_elapsed = time.time() - start_time
            timings = {"seconds_elapsed": int(time_elapsed)}
            if timings["seconds_elapsed"] > 60:
                timings["minutes_elapsed"] = int(timings["seconds_elapsed"] / 60)
                if timings["minutes_elapsed"] > 60:
                    timings["hours_elapsed"] = int(timings["minutes_elapsed"] / 60)

            self._log.debug("Update", keys_done=key_counter, **timings)

    def _get_ds_channels_from_fs(self, key):
        path_to_ds = self._key_to_path(key)
        root_path = os.path.join(*path_to_ds.split(os.path.sep)[:-1])
        ds_key = path_to_ds.split(os.path.sep)[-1]
        ds_reader = self._reader_class(
            root_path=root_path,
            manifest_key=os.path.join(ds_key, self._ds_type),
            log=self._log,
        )
        ds_loader = GameDsLoader(reader=ds_reader, log=self._log)
        data = ds_loader.get_channels(self._ds_reader_instructions)
        return data

    def _load(self):
        if self._loaded:
            return
        self.keyset = self._header_loader.get_keyset()
        header_dataframe = self._header_loader.get_dataframe()
        self._key_to_path_hashmap = header_dataframe.set_index("key").to_dict()[
            "ds_path"
        ]

    def _key_to_path(self, key: str) -> str:
        return self._key_to_path_hashmap[key]
