# pylint: disable=missing-docstring

import os
import zipfile
from pathlib import Path

from ..ds_io.game_ds_loader import GameDsLoader
from ..ds_io.reader_fs import DsReaderFs
from .unzip import extract_adx_zips

FIXTURES = Path("fixtures")
MATCH_KEY = "csds/2022/05/15/63cc7181-07c9-42fd-ade4-4eeb2cf4db6f"


def zip_match(root, match_key=MATCH_KEY):
    """Zip a fixture match the way csgo-dataexchange does: stored, full keys."""
    zip_path = root / f"{match_key}.zip"
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as archive:
        for path in sorted((FIXTURES / match_key).iterdir()):
            archive.write(path, f"{match_key}/{path.name}")
    return zip_path


def test_rebuilds_the_loose_layout(tmp_path):
    zip_match(tmp_path)

    assert extract_adx_zips(str(tmp_path)) == 1

    for path in (FIXTURES / MATCH_KEY).iterdir():
        extracted = tmp_path / MATCH_KEY / path.name
        assert extracted.read_bytes() == path.read_bytes()


def test_extracted_match_reads_like_the_loose_one(tmp_path):
    zip_match(tmp_path)
    extract_adx_zips(str(tmp_path))

    def load(root):
        reader = DsReaderFs(root_path=str(root), manifest_key=f"{MATCH_KEY}/csds")
        return GameDsLoader(reader=reader)

    loose, extracted = load(FIXTURES), load(tmp_path)
    assert extracted.manifest == loose.manifest
    channels = [{"channel": "player_death"}, {"channel": "round_end"}]
    for name, frame in extracted.get_channels(channels).items():
        assert frame.equals(loose.get_channels(channels)[name])


def test_prefix_and_delete_zips(tmp_path):
    zip_path = zip_match(tmp_path / "exports")

    assert extract_adx_zips(str(tmp_path), prefix="exports", delete_zips=True) == 1

    assert not zip_path.exists()
    assert (tmp_path / "exports" / MATCH_KEY / "csds").is_file()


def test_rerun_leaves_extracted_files_alone(tmp_path):
    zip_match(tmp_path)
    extract_adx_zips(str(tmp_path))
    manifest = tmp_path / MATCH_KEY / "csds"
    os.utime(manifest, (0, 0))

    assert extract_adx_zips(str(tmp_path)) == 1

    assert manifest.stat().st_mtime == 0


def test_skips_a_zip_that_is_not_a_match(tmp_path):
    other = tmp_path / "notes.zip"
    with zipfile.ZipFile(other, "w") as archive:
        archive.writestr("notes.txt", "hello")
    zip_match(tmp_path)

    assert extract_adx_zips(str(tmp_path)) == 1

    assert not (tmp_path / "notes.txt").exists()
    assert other.exists()


def test_skips_a_zip_with_an_entry_outside_its_match(tmp_path):
    zip_path = tmp_path / f"{MATCH_KEY}.zip"
    zip_path.parent.mkdir(parents=True)
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr(f"{MATCH_KEY}/csds", "{}")
        archive.writestr(f"{MATCH_KEY}/../escaped", "x")

    assert extract_adx_zips(str(tmp_path)) == 0

    assert not (tmp_path / MATCH_KEY).exists()
    assert not (tmp_path / "csds/2022/05/15/escaped").exists()
