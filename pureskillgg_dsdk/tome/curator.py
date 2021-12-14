import os

import structlog

from .header_tome import create_header_tome_from_fs, create_subheader_tome_from_fs


from .loader import TomeLoader
from .scribe import TomeScribe
from .manifest import TomeManifest
from .maker import TomeMaker
from .writer_fs import TomeWriterFs
from .reader_fs import TomeReaderFs


class TomeCuratorFs:
    def __init__(
        self,
        *,
        default_header_name=None,
        ds_type=None,
        tome_collection_root_path=None,
        ds_collection_root_path=None,
        log=None,
    ):
        self._log = log if log is not None else structlog.get_logger()
        self._default_header_name = get_env_option(
            "default_header_name", default_header_name
        )
        self._ds_type = get_env_option("ds_type", ds_type)
        self._tome_collection_root_path = get_env_option(
            "collection_path", tome_collection_root_path
        )
        self._ds_collection_root_path = get_env_option(
            "ds_collection_path", ds_collection_root_path
        )

    def create_header_tome(self, tome_name=None, /, *, path_depth=4):
        name = tome_name if tome_name is not None else self._default_header_name
        return create_header_tome_from_fs(
            name,
            ds_type=self._ds_type,
            tome_collection_root_path=self._tome_collection_root_path,
            ds_collection_root_path=self._ds_collection_root_path,
            path_depth=path_depth,
            log=self._log,
        )

    def create_subheader_tome(
        self, tome_name, selector=lambda df: True, /, *, src_tome_name=None
    ):
        src_name = (
            src_tome_name if src_tome_name is not None else self._default_header_name
        )
        return create_subheader_tome_from_fs(
            tome_name,
            src_tome_name=src_name,
            selector=selector,
            tome_collection_root_path=self._tome_collection_root_path,
            log=self._log,
        )

    def get_dataframe(self, tome_name):
        loader = self.get_loader(tome_name)
        return loader.get_dataframe()

    def get_keyset(self, tome_name):
        loader = self.get_loader(tome_name)
        return loader.get_keyset()

    def get_manifest(self, tome_name):
        loader = self.get_loader(tome_name)
        return loader.manifest

    def iterate_pages(self, tome_name):
        loader = self.get_loader(tome_name)
        return loader.iterate_pages()

    def get_loader(self, tome_name):
        reader = TomeReaderFs(
            root_path=self._tome_collection_root_path,
            tome_name=tome_name,
            log=self._log,
        )
        loader = TomeLoader(reader=reader, log=self._log)
        return loader

    def new_tome(
        self, tome_name, /, *, header_tome_name=None, ds_reader_instructions=None
    ):
        header_name = (
            header_tome_name
            if header_tome_name is not None
            else self._default_header_name
        )
        name = tome_name

        header_loader = self.get_loader(header_name)

        writer = TomeWriterFs(
            root_path=self._tome_collection_root_path, tome_name=name, log=self._log
        )
        manifest = TomeManifest(tome_name=name, path=writer.path, ds_type=self._ds_type)
        scribe = TomeScribe(manifest=manifest, writer=writer, log=self._log)

        tomer = TomeMaker(
            header_loader=header_loader,
            scribe=scribe,
            ds_reader_instructions=ds_reader_instructions,
            ds_type=self._ds_type,
        )
        return tomer


def get_env_option(name, value):
    if value is not None:
        return value
    env_prefix = "pureskillgg_tome"
    key = "_".join([env_prefix, name]).upper()
    env_value = os.environ.get(key)
    if env_value is not None:
        return env_value
    raise Exception(f"Missing option {name}, pass in or set {key} in environment")
