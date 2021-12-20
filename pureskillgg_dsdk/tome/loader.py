import structlog


class TomeLoader:
    def __init__(self, *, reader, has_header=True, log: object = None):
        self._reader = reader
        self._log = log if log is not None else structlog.get_logger()
        self._manifest = None
        self._metadata = None
        self._exists = None
        self.has_header = has_header
        self.header = None

        if self.has_header:
            self.header = TomeLoader(
                reader=self._reader.header, has_header=False, log=self._log
            )

    @property
    def metadata(self):
        """Metadata"""
        self._load()
        return self._metadata

    @property
    def exists(self):
        """If the tome exists"""
        return self._reader.exists

    @property
    def manifest(self):
        """Tome manifest"""
        self._load()
        return self._manifest

    def _load(self) -> None:
        is_loaded = self._manifest is not None and self._metadata is not None
        if is_loaded:
            return
        self._metadata = self._reader.read_metadata()
        self._manifest = self._reader.read_manifest()

    def get_dataframe(self):
        self._load()

        df = None
        for page in self.manifest["pages"]:
            df_temp = self._reader.read_page_dataframe(page)
            if df is None:
                df = df_temp
            else:
                df = df.append(df_temp)
        return df

    def get_keyset(self):
        self._load()

        keyset = []
        for page in self.manifest["pages"]:
            keyset += self._reader.read_page_keyset(page)
        return keyset

    def iterate_pages(self):
        self._load()
        for page in self.manifest["pages"]:
            yield (
                self._reader.read_page_dataframe(page),
                self._reader.read_page_keyset(page),
            )
