# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## 0.7.0 / 2021-12-20

### Added

- Tome maker behaviors when tome exists: continue, overwrite, pass, or fail
- Tome maker behaviors work when tome is complete or incomplete.
- New tomes now copy the header or subheader used to create.
- Copied headers have a `sourceId` set
- New tests in `curator_test` for make tome and it's behaviors when tomes exist.
- Tome loader now has an `exists` property that is read from the reader.

### Changed

- `new_tome` function replaced in curator with `make_tome` function.
- Tome curator test now uses the `tmp_path` from pytest to save new files.
- Tome manifest data can be set which is used when continuing a tome.
- Tome reader and tome loader both have `header` properties that mirror their own class properties.

### Fixed

- Tome maker uses pathlib to find parent path because os.join did not work correctly when base path was a drive on windows.
- Scribe correctly writes last page in certain cases.
- Tome writer can write multiple nested folders if needed.


## 0.6.0 / 2021-12-13

### Added

- Tome maker.

## 0.5.0 / 2021-09-27

### Changed

- Game ds io modules can now be imported from top level.

## 0.4.0 / 2021-09-27

### Fixed

- Issue with structlog version.

## 0.3.0 / 2021-09-13

### Added

- Game ds io modules.

## 0.2.0 / 2021-06-30

### Added

- Scikit learn model to ds models.

## 0.1.0 / 2021-02-05

### Added

- Data science models.
