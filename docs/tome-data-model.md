# Tome / channel dataset data model

This is the dataset abstraction `pureskillgg_dsdk` standardizes. It is what
`csgo-dsdk`, `csgo-datascience`, the coach, and progression all build training
data on top of. Everything below lives in `pureskillgg_dsdk/ds_io` and
`pureskillgg_dsdk/tome`.

## The "ds" object (parsed match)

A parsed demo is stored as a **"ds" object** (default `ds_type` `csds`), produced
upstream by the replay / csds stage. Each ds object is:

- a JSON **manifest** describing the object and its **channels**, and
- one parquet payload **per channel**.

`GameDsLoader` (wrapping `DsReaderS3` or `DsReaderFs`) is the reader:

- `get_channels()` returns the channels declared in the manifest.
- `get_channel(name)` looks the channel up in the manifest and reads its parquet
  payload into a pandas DataFrame. Only `application/x-parquet` channels are
  supported.

`DsReaderS3` notes:

- The manifest is fetched as JSON and is **gzip-aware** (honors
  `ContentEncoding`).
- Object metadata comes from S3 `head_object`.
- Channel parquet is read via `pd.read_parquet`, with a fallback that re-reads
  the raw bytes through boto3 to work around a flaky Arrow/S3
  `FileNotFoundError`. Parquet `ValueError` is funneled through
  `handle_value_error`.
- The **bucket** is a constructor argument; the reader resolves keys under an
  optional `prefix`. The library never hardcodes a bucket.

`normalize_instructions` merges multiple read instructions for the same channel:
the union of requested `columns`, or **all** columns if any instruction omits
`columns`.

## Tomes (page-chunked datasets across many matches)

A **tome** aggregates many matches into a page-chunked parquet dataset plus a
manifest. `TomeCuratorFs` (filesystem) is the high-level API. Its paths and
`ds_type` come from `PURESKILLGG_TOME_*` env vars (or constructor args).

Kinds of tome:

- **Header tome** — one row per match, scanned from a ds collection on disk via
  glob. Created with `create_header_tome`.
- **Subheader tome** — a filtered header, produced with a selector, via
  `create_subheader_tome`.
- **Data tome** — the actual training data. `make_tome` iterates a header's
  keyset and concatenates per-channel DataFrames into pages.

Reading back:

- `get_dataframe`, `get_keyset`, `get_manifest`, `iterate_pages`
- `get_match_by_index`, `get_random_match`

## make_tome resume / overwrite state machine

`TomeMaker.make_tome` is the subtle part. It branches on whether a tome already
exists and whether it is complete (`isComplete`), combined with
`behavior_if_complete` and `behavior_if_partial` — each one of
**continue / overwrite / pass / fail**:

- **overwrite** rebuilds from scratch.
- **fail** raises if the relevant state is present.
- **pass** leaves the existing tome alone.
- **continue** computes the remaining work as
  `header_keyset - existing_keyset` and only builds those matches.
- Special case: a **complete** tome with **continue** degrades to a passthrough
  (nothing to do).

## TomeScribe paging and manifest bookkeeping

`TomeScribe` writes pages; `TomeManifest` records them. Gotchas:

- Page splitting only triggers on a `limit_check_frequency` boundary **and** only
  when `max_page_size_mb` or `max_page_row_count` is set.
- The size check is **in-memory** size, which runs 2-10x larger than the parquet
  on disk — budget pages accordingly.
- `TomeManifest` builds the keyset and dataframe parquet keys and records
  per-page and total timings.
- An **empty tome is not supported** ("Empty Tome not supported"), and there is a
  non-obvious copied-header key path to be aware of when reading the code.
