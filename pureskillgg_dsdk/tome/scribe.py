import structlog
import pandas as pd


class TomeScribe:
    def __init__(self, *, manifest, writer, log: object = None):
        self._log = log if log is not None else structlog.get_logger()
        self._manifest = manifest
        self._writer = writer
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

    def start(self):
        self._writer.write_manifest(self._manifest.get())

    def finish(self):
        if len(self._keyset) > 0:
            self._write()

    def concat(self, df, keys):
        self._concat_keys(keys)
        self._concat_df(df)

    def _write(self):
        page = self._manifest.add_page(self._page_counter)
        self._writer.write_page(page, self.dataframe, self.keyset)
        self._writer.write_manifest(self._manifest.get())
        self._page_counter += 1
        self._reset_data_keyset()

    def _reset_data_keyset(self) -> None:
        self._keyset = []
        self._data_dict = {}
        self._data_dict_index = 0
        self._data_df = None

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
