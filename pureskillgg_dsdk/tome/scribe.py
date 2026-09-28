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

        self._frames = []
        self._unmeasured = []
        self._page_bytes = 0
        self._keyset = []
        self._data_df = None
        self._page_counter = 0

    @property
    def dataframe(self):
        if self._data_df is None:
            self._data_df = self._build_page()
        return self._data_df

    @property
    def keyset(self):
        return self._keyset

    @property
    def tome_name(self):
        return self._manifest.get()["tome"]

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
        self._frames = []
        self._unmeasured = []
        self._page_bytes = 0
        self._data_df = None
        self._manifest.start_page()

    def _concat_df(self, df):
        # A frame with no rows adds nothing to the page. Skipping it keeps its
        # columns and dtypes out of the page, as before.
        if df is None or len(df) == 0:
            return
        self._data_df = None
        # Copy, so later changes to the caller's frame don't reach the page.
        frame = df.copy()
        self._frames.append(frame)
        self._unmeasured.append(frame)

    def _build_page(self):
        if len(self._frames) == 0:
            return pd.DataFrame()
        page = pd.concat(self._frames, ignore_index=True)
        # The page now holds every row; drop the parts so memory isn't doubled.
        self._frames = [page]
        if len(self._unmeasured) > 0:
            # Don't keep unmeasured parts alive either: the next size check
            # measures the whole page instead.
            self._unmeasured = [page]
            self._page_bytes = 0
        return page

    def _concat_keys(self, keys):
        if isinstance(keys, list):
            self._keyset += keys
        else:
            self._keyset.append(keys)

    def _get_page_size_mb(self) -> float:
        self._measure_new_frames()
        return self._page_bytes / 1024 / 1024

    def _measure_new_frames(self) -> None:
        # Measure only the frames added since the last check, in one
        # memory_usage call, and keep a running total for the page. Measuring
        # the whole page at every check made each check slower as the page
        # grew. deep=True counts string contents, so string-heavy pages still
        # split early. The total equals the built page's
        # memory_usage(index=False, deep=True), unless frames measured at
        # different checks disagree on a column's dtype and pd.concat
        # converts it.
        frames = self._unmeasured
        if len(frames) == 0:
            return
        new = frames[0] if len(frames) == 1 else pd.concat(frames, ignore_index=True)
        self._page_bytes += int(new.memory_usage(index=False, deep=True).sum())
        self._unmeasured = []

    def _get_page_row_count(self) -> int:
        return sum(len(df) for df in self._frames)
