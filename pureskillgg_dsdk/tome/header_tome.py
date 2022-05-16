import os
from glob import glob
import structlog

from ..ds_io import DsReaderFs, GameDsLoader
from .loader import TomeLoader
from .scribe import TomeScribe
from .manifest import TomeManifest
from .writer_fs import TomeWriterFs
from .reader_fs import TomeReaderFs
from .constants import filter_ds_reader_logs


def default_tome_name():
    return "header"


# pylint: disable=unused-argument
def create_header_tome_from_fs(
    tome_name=None,
    /,
    *,
    ds_type="csds",
    tome_collection_root_path="tomes",
    ds_collection_root_path="data",
    path_depth=None,
    log=None,
):
    """Make the header tome"""
    name = default_tome_name() if tome_name is None else tome_name
    log = (
        log
        if log is not None
        else structlog.wrap_logger(
            structlog.get_logger(), processors=[filter_ds_reader_logs]
        )
    )

    writer = TomeWriterFs(root_path=tome_collection_root_path, tome_name=name, log=log)
    tome_manifest = TomeManifest(
        tome_name=name, path=writer.path, ds_type=ds_type, is_header=True
    )
    scribe = TomeScribe(manifest=tome_manifest, writer=writer, log=log)

    scribe.start()

    for manifest_key_path in get_manifest_key_paths_from_glob(
        ds_collection_root_path, ds_type
    ):
        ds_loader = fetch_ds_loader_from_fs(
            ds_collection_root_path, manifest_key_path, log
        )
        df = ds_loader.get_channel({"channel": "header"})
        df["key"] = ds_loader.manifest["key"]
        df["match_id"] = ds_loader.manifest["id"]
        scribe.concat(df, ds_loader.manifest["key"])

    scribe.finish()

    reader = TomeReaderFs(root_path=tome_collection_root_path, tome_name=name, log=log)

    return TomeLoader(reader=reader, log=log)


# pylint: disable=too-many-locals
def create_subheader_tome_from_fs(
    name,
    /,
    src_tome_name=None,
    selector=lambda df: [True] * len(df),
    *,
    tome_collection_root_path="tomes",
    dest_tome_name=None,
    preserve_src_id=False,
    log=None,
):
    """
    selector is passed to filter out rows from the header
    by doing df = df.loc[selector]
    """
    log = log if log is not None else structlog.get_logger()
    src_name = default_tome_name() if src_tome_name is None else src_tome_name

    src_reader = TomeReaderFs(
        root_path=tome_collection_root_path, tome_name=src_name, log=log
    )
    src_loader = TomeLoader(reader=src_reader, log=log)

    if dest_tome_name is None:
        output_path = tome_collection_root_path
    else:
        output_path = os.path.join(tome_collection_root_path, dest_tome_name)

    writer = TomeWriterFs(root_path=output_path, tome_name=name, log=log)
    manifest = TomeManifest(
        tome_name=name,
        path=writer.path,
        ds_type=src_loader.manifest["dsType"],
        header_tome_name=src_name,
        is_header=True,
        src_id=src_loader.manifest["id"] if preserve_src_id else None,
    )
    scribe = TomeScribe(writer=writer, manifest=manifest, log=log)
    scribe.start()
    df = src_loader.get_dataframe().loc[selector]
    scribe.concat(df, list(df["key"]))
    scribe.finish()

    reader = TomeReaderFs(root_path=output_path, tome_name=name, log=log)

    return TomeLoader(reader=reader, log=log)


def get_manifest_key_paths_from_glob(ds_root_path, ds_type):
    root_path = os.path.normpath(ds_root_path) + os.sep
    paths = glob(os.path.join(root_path, "*", "**", ds_type), recursive=True)
    paths = [os.path.normpath(path) for path in paths]
    manifest_keys = [
        path[len(os.path.commonprefix([root_path, path])) :] for path in paths
    ]
    manifest_keys = set(manifest_keys)
    return manifest_keys


def fetch_ds_loader_from_fs(root_path, manifest_key, log):
    ds_reader = DsReaderFs(
        root_path=root_path,
        manifest_key=manifest_key,
        log=log,
    )
    ds_loader = GameDsLoader(reader=ds_reader, log=log)
    return ds_loader
