# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## 3.2.2

### Changed

- `TomeScribe` builds pages with `pd.concat`: about 90 times faster, and columns keep their dtypes (`category`, nullable `Int64`, `int8`).
- `max_page_size_mb` counts the contents of string columns, so string-heavy pages split sooner.
- Depend on `pandas[parquet,fss,aws]` only: the unused `performance`, `plot`, `output-formatting` and `computation` extras (numba, matplotlib, xarray and more) are gone.
- Allow pyarrow 25.
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
