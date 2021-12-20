import structlog
import pandas as pd


class TomeScribe:
    def __init__(
        self,
        *,
        manifest,
        writer,
        max_page_size_mb=None,
        max_page_row_count=None,
        limit_check_frequency=100,
        log: object = None,
    ):
        self._log = log if log is not None else structlog.get_logger()
        self._manifest = manifest
        self._writer = writer
        self._max_page_size_mb = max_page_size_mb
        self._max_page_row_count = max_page_row_count
        self._limit_check_frequency = limit_check_frequency

        self._data_dict = {}
        self._data_dict_index = 0
        self._keyset = []
        self._data_df = None
        self._page_counter = 0

    @property
    def dataframe(self):
        if self._data_df is None:
            self._data_df = pd.DataFrame.from_dict(self._data_dict, "index")
        return self._data_df

    @property
    def keyset(self):
        return self._keyset

    @property
    def path(self):
        return self._writer.path

    @property
    def tome_name(self):
        return self._writer.tome_name

    @property
    def page_counter(self):
        return self._page_counter

    @property
    def page_size_mb(self):
        return self._get_page_size_mb()

    @property
    def page_row_count(self):
        return self._get_page_row_count()

    def start(self):
        self._writer.write_manifest(self._manifest.get())
        self._manifest.start_page()

    def finish(self):
        if self._page_counter == 0 and len(self._keyset) == 0:
            raise Exception("Empty Tome not supported")
        if len(self._keyset) > 0:
            self._write()
        self._manifest.finish()
        self._writer.write_manifest(self._manifest.get())

    def concat(self, df, keys):
        self._concat_keys(keys)
        self._concat_df(df)
        self._on_data()

    def set_manifest_data(self, data):
        self._page_counter = len(data["pages"])
        self._manifest.set(data)

    def _write(self):
        page = self._manifest.end_page(self._page_counter)
        self._writer.write_page(page, self.dataframe, self.keyset)
        self._writer.write_manifest(self._manifest.get())
        self._page_counter += 1
        self._new_page()

    def _on_data(self):
        if self._will_write_page():
            self._write()

    def _will_write_page(self) -> bool:
        if len(self.keyset) == 0:
            return False
        if len(self.keyset) % self._limit_check_frequency != 0:
            return False
        if self._max_page_size_mb is not None:
            current_size = self._get_page_size_mb()
            if current_size > self._max_page_size_mb:
                return True
        if self._max_page_row_count is not None:
            current_row_count = self._get_page_row_count()
            if current_row_count > self._max_page_row_count:
                return True
        return False

    def _new_page(self) -> None:
        self._keyset = []
        self._data_dict = {}
        self._data_dict_index = 0
        self._data_df = None
        self._manifest.start_page()

    def _concat_df(self, df):
        if df is not None:
            self._data_df = None
            temp_dict = df.to_dict(orient="records")
            for entry in temp_dict:
                self._data_dict[self._data_dict_index] = entry
                self._data_dict_index += 1

    def _concat_keys(self, keys):
        if isinstance(keys, list):
            self._keyset += keys
        else:
            self._keyset.append(keys)

    def _get_page_size_mb(self) -> float:
        df = self.dataframe
        size_in_mb = sum(df.memory_usage()) / 1024 / 1024
        return size_in_mb

    def _get_page_row_count(self) -> int:
        df = self.dataframe
        row_count = df.shape[0]
        return row_count
