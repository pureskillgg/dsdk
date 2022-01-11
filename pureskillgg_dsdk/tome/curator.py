import os
import pathlib
import structlog
import pandas as pd

from .header_tome import create_header_tome_from_fs, create_subheader_tome_from_fs

from .loader import TomeLoader
from .scribe import TomeScribe
from .manifest import TomeManifest
from .maker import TomeMaker
from .writer_fs import TomeWriterFs
from .reader_fs import TomeReaderFs


class TomeCuratorFs:
    """
    Simple API to manage tomes.

    Parameters
    ----------
    default_header_name : str, default=from env (PURESKILLGG_TOME_DEFAULT_HEADER_NAME)
        Name of the default header.
    ds_type : str, default=from env (PURESKILLGG_TOME_DS_TYPE)
        Type of data science file to read from `ds_collection_root_path`.
    tome_collection_root_path : str, default=from env (PURESKILLGG_TOME_COLLECTION_PATH)
        Path leading to where tomes will be stored.
    ds_collection_root_path : str, default=from env (PURESKILLGG_TOME_DS_COLLECTION_PATH)
        Path leading to a series of (possibly nested) folders containing game Data Science files.
    log : structlog.stdlib.BoundLogger
        Logger used for logging logs.

    See Also
    --------
    TomeMaker : Make tomes.
    """

    def __init__(
        self,
        *,
        default_header_name: str = None,
        ds_type: str = None,
        tome_collection_root_path: str = None,
        ds_collection_root_path: str = None,
        log: structlog.stdlib.BoundLogger = None,
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

    def create_header_tome(
        self, tome_name: str = None, /, *, path_depth: int = 4
    ) -> TomeLoader:
        """
        Create the header tome.

        Parameters
        ----------
        tome_name : str, default=`default_header_name`
            Name of the header that will be created.
        path depth : int, default=4
            Folder depth to search for game Data Science files.

        Returns
        -------
        TomeLoader
            Loader for the header tome just created.
        """
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
        self,
        tome_name: str,
        selector: callable = lambda df: [True] * len(df),
        /,
        *,
        src_tome_name: str = None,
    ) -> TomeLoader:
        """
        Create a subheader tome.

        Parameters
        ----------
        tome_name : str, default=`default_header_name`
            Name of the header that will be created.
        selector : callable, default=lambda to select all rows
            The selector is passed directly through to the header
            dataframe and the final subheader tome will be equal to
            `header_dataframe.loc[selector]`.
        src_tome_name : str, default=`default_header_name`
            Source header file. Should be the same as the
            default header name in most cases.

        Returns
        -------
        TomeLoader
            Loader for the header tome just created.
        """
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

    def get_dataframe(self, tome_name: str) -> pd.DataFrame:
        """
        Get the dataframe from a tome.

        Parameters
        ----------
        tome_name : str
            Name of the tome.

        Returns
        -------
        pd.DataFrame
            Pandas dataframe containing the tome's data.
        """
        loader = self.get_loader(tome_name)
        return loader.get_dataframe()

    def get_keyset(self, tome_name: str) -> list:
        """
        Get the keyset from a tome.

        Parameters
        ----------
        tome_name : str
            Name of the tome.

        Returns
        -------
        list
            List containing the tome's keyset.
        """
        loader = self.get_loader(tome_name)
        return loader.get_keyset()

    def get_manifest(self, tome_name: str) -> dict:
        """
        Get the manifest from a tome.

        Parameters
        ----------
        tome_name : str
            Name of the tome.

        Returns
        -------
        dict
            Dictionary containing the tome's manifest.
        """
        loader = self.get_loader(tome_name)
        return loader.manifest

    def iterate_pages(self, tome_name: str) -> TomeLoader.iterate_pages:
        """
        Iterate through pages of a tome.

        Parameters
        ----------
        tome_name : str
            Name of the tome.

        Returns
        -------
        TomeLoader.iterate_pages
            Iterator for pages from TomeLoader.
        """
        loader = self.get_loader(tome_name)
        return loader.iterate_pages()

    def get_loader(self, tome_name: str) -> TomeLoader:
        """
        Get the loader for a tome.

        Parameters
        ----------
        tome_name : str
            Name of the tome.

        Returns
        -------
        TomeLoader
            The TomeLoader instance for this tome.
        """
        reader = TomeReaderFs(
            root_path=self._tome_collection_root_path,
            tome_name=tome_name,
            log=self._log,
        )
        loader = TomeLoader(reader=reader, log=self._log)
        return loader

    # pylint: disable=too-many-locals
    def make_tome(
        self,
        tome_name: str,
        /,
        *,
        header_tome_name: str = None,
        ds_reading_instructions=None,
        max_page_size_mb=None,
        max_page_row_count=None,
        behavior_if_complete="pass",
        behavior_if_partial="continue",
        limit_check_frequency=100,
    ):
        header_name = (
            header_tome_name
            if header_tome_name is not None
            else self._default_header_name
        )
        name = tome_name

        existing_tome_loader = self.get_loader(name)

        header_loader = self.get_loader(header_name)

        writer = TomeWriterFs(
            root_path=self._tome_collection_root_path, tome_name=name, log=self._log
        )
        manifest = TomeManifest(
            tome_name=name,
            path=writer.path,
            ds_type=self._ds_type,
            header_tome_name=header_name,
            log=self._log,
        )
        scribe = TomeScribe(
            manifest=manifest,
            writer=writer,
            max_page_size_mb=max_page_size_mb,
            max_page_row_count=max_page_row_count,
            limit_check_frequency=limit_check_frequency,
            log=self._log,
        )

        tomer = TomeMaker(
            header_loader=header_loader,
            scribe=scribe,
            ds_reading_instructions=ds_reading_instructions,
            ds_type=self._ds_type,
            tome_loader=existing_tome_loader,
            copy_header=copy_header_func,
            behavior_if_complete=behavior_if_complete,
            behavior_if_partial=behavior_if_partial,
            log=self._log,
        )
        return tomer


def copy_header_func(header_loader, scribe, log):
    path = str(pathlib.Path(header_loader.manifest["path"]).parent)
    create_subheader_tome_from_fs(
        "header",
        src_tome_name=header_loader.manifest.get("tome"),
        tome_collection_root_path=path,
        dest_tome_name=scribe.tome_name,
        preserve_src_id=True,
        log=log,
    )


def get_env_option(name, value):
    if value is not None:
        return value
    env_prefix = "pureskillgg_tome"
    key = "_".join([env_prefix, name]).upper()
    env_value = os.environ.get(key)
    if env_value is not None:
        return env_value
    raise Exception(f"Missing option {name}, pass in or set {key} in environment")
