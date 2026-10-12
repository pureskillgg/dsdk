"""Unzip matches delivered as one zip each back into the loose layout.

Since October 2026 the data set delivers each match as one asset,
``csds/YYYY/MM/DD/<id>.zip``, instead of one per channel. Its entries are the
match's files under their full keys (``csds/YYYY/MM/DD/<id>/csds``,
``csds/YYYY/MM/DD/<id>/<channel>``), stored without recompression, so
extracting them into the folder the keys start from rebuilds the layout that
DsReaderFs and the tome builders read.
"""

import os
import shutil
import zipfile

import structlog


def extract_adx_zips(root_path, /, *, prefix=None, delete_zips=False, log=None):
    """Extract every match zip under a folder into the loose layout.

    ``root_path`` (joined with ``prefix``, as the download and export functions
    take it) is the folder the keys start from: the one holding ``csds/``.
    Each ``<id>.zip`` found under it is extracted next to itself, as
    ``<id>/csds`` and ``<id>/<channel>``. A file already there with the same
    size is left alone, so a rerun only finishes what an interrupted one
    started. With ``delete_zips``, each zip is removed once it is extracted.

    Returns the number of zips extracted.
    """
    log = log if log is not None else structlog.get_logger()
    root = root_path if prefix is None else os.path.join(root_path, prefix)
    root = os.path.abspath(root)
    log = log.bind(client="adx_extract_zips", root_path=root)

    count = 0
    for zip_path in sorted(_find_zips(root)):
        try:
            extracted = _extract_zip(root, zip_path)
        except zipfile.BadZipFile as err:
            # A truncated or corrupt zip stops only itself, not the scan.
            log.warning("Skipped Unreadable Zip", path=zip_path, error=str(err))
            continue
        if not extracted:
            log.warning("Skipped Zip That Is Not A Match", path=zip_path)
            continue
        if delete_zips:
            os.remove(zip_path)
        count += 1
        log.info("Extracted Match Zip", path=zip_path)
    return count


def _find_zips(root):
    for dirpath, _dirnames, filenames in os.walk(root):
        for filename in filenames:
            if filename.endswith(".zip"):
                yield os.path.join(dirpath, filename)


def _extract_zip(root, zip_path):
    """Extract one match zip; False, with nothing written, if it isn't one."""
    # Every entry must sit in the match's own folder, the zip's key without
    # ".zip", so a zip can't write anywhere else.
    match_key = os.path.relpath(zip_path, root)[: -len(".zip")].replace(os.sep, "/")
    with zipfile.ZipFile(zip_path) as archive:
        entries = [info for info in archive.infolist() if not info.is_dir()]
        if not entries or not all(
            _is_in_match(info.filename, match_key) for info in entries
        ):
            return False
        for info in entries:
            target = os.path.join(root, *info.filename.split("/"))
            if os.path.isfile(target) and os.path.getsize(target) == info.file_size:
                continue
            os.makedirs(os.path.dirname(target), exist_ok=True)
            partial = target + ".partial"
            try:
                with archive.open(info) as source, open(partial, "wb") as sink:
                    shutil.copyfileobj(source, sink)
            except BaseException:
                os.remove(partial)
                raise
            os.replace(partial, target)
    return True


def _is_in_match(name, match_key):
    parts = name.split("/")
    return (
        name.startswith(match_key + "/")
        and "\\" not in name
        and ".." not in parts
        and "" not in parts
    )
