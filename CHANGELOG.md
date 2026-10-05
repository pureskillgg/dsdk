# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## 4.0.0

### Added

- `widen` argument on `TomeLoader.get_dataframe`, `TomeLoader.scan`, `TomeCuratorFs.get_dataframe` and `TomeCuratorFs.scan`: every integer column comes back as a 64-bit integer (`Int16` as `Int64`) and every float column as float64, with the values of the default load. It is for analysis code: narrow integers wrap in element-wise arithmetic (an int16 `money * 5` gives 14,464 for 16,000).
- `pureskillgg_dsdk.tome.NarrowingError`, raised when a value doesn't fit the type its column is narrowed to.

### Changed

- **Tomes that mix old csds files with csgo-ppp's compact player tables are narrowed, not widened.** Where some matches or pages hold a column as int64 or float64 and others as int8, int16, int32 or float32, `build_basic_tomes` and the loaders cast the wide values to the narrow type. The casts are checked: a value that doesn't fit raises `NarrowingError`, naming the column, the match (or the page) and the value. `current_ammo` stays int64 (the compact files' int16 is widened to it, on every page), since older files hold 4294967295 there. A float64 narrowed to float32 is rounded to the nearest float32; no other value changes. A narrow integer column with missing values is pandas' nullable `Int8`, `Int16` or `Int32`, not float64. At the end, `build_basic_tomes` casts any page not yet at the tome's types (one written before the first compact match, or before a wider narrow type turned up) and writes it again, so every page holds the tome's types. Tomes of old files only that built before build and load exactly as before. docs/tome-data-model.md has the rules.
- **Flags held as 0 and 1 in some files and as bool in others are narrowed to bool.** Older csds files hold `burst_mode` and `is_silenced` (`player_status`) and flags in `player_death`, `other_death` and `bomb_defuse` as int64 declared `Int64`; newer ones hold bool. A page holding both couldn't be written (`pd.concat` made an object column of bools and ints, which pyarrow refused), so `build_basic_tomes` failed on tomes spanning those csds versions. Now the integers are cast to bool with a check: 0 and 1 become False and True, a missing value stays missing (`boolean` in pandas), and any other value raises `NarrowingError`. `widen=True` leaves flags bool. `make_tome` still joins a page's frames with `pd.concat`, so it still fails on such a page.
- `build_basic_tomes` decodes dictionary (category) columns, such as the compact `place_name`, as it reads each match, so their pages are built with pyarrow instead of falling back to pandas. pandas reads them as strings, which is what `pd.concat` gives categories that differ between matches.

## 3.3.1

### Added

- `TomeCuratorFs.build_basic_tomes(channels)` (and `build_basic_tomes_from_fs`) builds the header tome and one tome per channel in one walk: each match is visited once, for its manifest, its header row and every channel, read with pyarrow by 4 threads. Each channel's tome holds the matches that have the channel, with their rows tagged `match_key`, and loads with the same values and dtypes as a `make_tome` tome over a subheader of those matches. A match key listed twice is built once. Complete tomes are kept, so a rerun reads nothing; an interrupted build leaves no partial channel tome and starts over.
- `TomeWriterFs.write_page` also takes a `pyarrow.Table`.
- `columns` argument on `TomeLoader.get_dataframe` and `TomeCuratorFs.get_dataframe`: read only these columns, in this order.
- A `polars` extra, `pureskillgg-dsdk[polars]`. With it, `get_dataframe(library="polars")` returns a polars DataFrame, and `TomeLoader.scan()` and `TomeCuratorFs.scan()` return a LazyFrame, so a query reads only the columns and rows it needs. Old pages' pandas index column is dropped, and pages whose columns differ are joined with `pl.concat(how="diagonal_relaxed")`. Without the extra, these raise an `ImportError` that names it.

### Changed

- **The pandas frame from `get_dataframe` has one `RangeIndex`, 0 to n - 1.** Before, each page's index was repeated, so the same labels came back once per page, and pages written by dsdk 3.2.1 and earlier restored the index they stored. The old index is not kept as a column. Code that picked rows of a tome frame by label (`.loc`) should pick them by position.
- `get_dataframe` reads a tome's pages with one pyarrow dataset scan and converts them to pandas once, instead of reading each page with `pd.read_parquet` and joining them with `pd.concat`. A 6.1M-row tome loads 2.0x faster on pandas 2.3 and 2.8x on pandas 3 with the files cached. Values and dtypes are unchanged. A column whose type differs between pages (beyond `int64` against `Int64`, or `int64` against `float64`), or that some pages lack, is still read page by page and joined with `pd.concat`, so it keeps the dtype it had.

## 3.3.0

### Added

- `compression` argument on `make_tome`, `create_header_tome`, `create_subheader_tome` and `TomeWriterFs`: the parquet codec for new tome pages. It takes any codec `DataFrame.to_parquet` accepts, and an unknown one fails before any page is written.

### Changed

- New tome pages are written with zstd instead of gzip. pyarrow writes gzip at level 9, which was about 95% of the time to build a large tome: a 17.4M-row page writes in 5 s with zstd against 119 s with gzip, and is 4% smaller. Pass `compression="gzip"` for the old format. Existing gzip tomes read unchanged, and a tome continued with a different codec reads back whole.
- The `max_page_size_mb` check measures only the frames added since the previous check, not the whole page every time; when new frames change the page's columns or dtypes, it measures the whole page as before. Pages split at the same points; the measure now leaves out the page's index (about 130 bytes).

## 3.2.2

### Changed

- `TomeScribe` builds pages with `pd.concat`: about 90 times faster, and columns keep their dtypes (`category`, nullable `Int64`, `int8`).
- `max_page_size_mb` counts the contents of string columns, so string-heavy pages split sooner.
- Depend on `pandas[parquet,fss,aws]` only: the unused `performance`, `plot`, `output-formatting` and `computation` extras (numba, matplotlib, xarray and more) are gone.
- Allow pyarrow 25.
- Allow pandas 3. On pandas 3, strings are read as the new `str` dtype, and tome pages write them as Arrow `large_string`.
- Declare `python-dateutil`, which the ADX module imports.

### Fixed

- `make_tome` with `behavior_if_complete` or `behavior_if_partial` set to `fail` raises "Tome already exists" instead of an `AttributeError`.

## 3.2.1

### Changed

- Harden the deploy workflows.
- Update GitHub Actions to Node.js 24 runtimes.

### Fixed

- `DsReaderS3` reads manifests stored without `ContentEncoding`.
- Header tomes list their matches in sorted order, and continued tomes keep the header's order.
- `create_header_tome_from_fs` works without a tome name.
- `find_matching_model` reports how many models matched.
- `s3_dataframe_set` names the unknown `res_type` in its error.
- `s3_dataframe_set` and `s3_scikit_set` download their model once; `sagemaker_endpoint` creates its client once.
- `AdxDataset` fetches the dataset once and logs export failures with `exc_info`.
- Import `dateutil.parser` and `urllib.request` explicitly in the ADX modules.
- Parquet read errors keep the original `ValueError` as their cause.
- `GameDsLoader.get_channel` is annotated as returning a `DataFrame`.

## 3.2.0

### Added

- `s3_xgboost` support for `model_type: Booster`.
- `s3_dataframe` support for `res_type: application/x-parquet`.

## 3.1.0 / 2026-07-13

### Added

- `s3_xgboost` ds-model type. Requires the `xgboost` extra (`pureskillgg-dsdk[xgboost]`).

## 3.0.1 / 2026-06-14

### Fixed

- Stage uv.lock in the version commit.

## 3.0.0 / 2026-06-14

- Migrate to Python 3.11+ (CI test matrix 3.11-3.14).
- Update the data stack: pandas 2.3, numpy 2, pyarrow 16-24, boto3 1.43, structlog 26, python-rapidjson 1.23.
- Add `pureskillgg_dsdk.sqs`, an async SQS consumer replacing `loafer` in the worker services.
- Update dev tooling: black 26, pylint 4, pytest 9, pytest-cov 7; remove pytest-runner.

## 2.0.0 / 2024-04-01

- Upgrade to pandas 2 and a newer boto3.

## 1.3.1 / 2024-03-31

- Downgrade boto3.

## 1.3.0 / 2024-03-31

- Update dependencies ahead of the pandas 2 upgrade.

## 1.2.0 / 2023-11-17

- Update pyarrow from 8.x to 14.x so that the [wheels are there](https://arrow.apache.org/install/)

## 1.0.3 / 2022-07-26

- No changes.

## 1.0.2 / 2022-07-25

- Initial release.
