# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## 3.2.0

### Added

- `s3_xgboost` accepts `model_type: Booster`: loads the artifact with
  `xgboost.Booster` and predicts through a `DMatrix` built with
  `enable_categorical=True` — serves regressors (e.g. `survival:aft` or
  percentile-target models saved via `save_model`) and softprob classifiers
  through one code path, including models with pandas categorical features.
- `s3_dataframe` accepts `res_type: application/x-parquet` for parquet
  artifacts (large lookup tables where CSV is impractical).

## 3.1.0 / 2026-07-13

### Added

- New `s3_xgboost` ds-model type: loads an XGBoost model saved with
  `save_model("*.json")` from S3 (`res_type: application/json`) and invokes
  `predict_proba` (`model_type: XGBClassifier`). Requires the new `xgboost`
  extra (`pureskillgg-dsdk[xgboost]`) — kept out of the base dependencies so
  consumers that never load models don't ship the xgboost wheel.

## 3.0.1 / 2026-06-14

### Fixed

- Stage uv.lock in the version commit.

## 3.0.0 / 2026-06-14

- Migrate to Python 3.11+ (CI test matrix 3.11-3.14).
- Update the data stack: pandas 2.3, numpy 2, pyarrow 16-24, boto3 1.43, structlog 26, python-rapidjson 1.23.
- Add `pureskillgg_dsdk.sqs`, an async SQS consumer (built on aiobotocore) that replaces the abandoned `loafer` package in the worker services.
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
