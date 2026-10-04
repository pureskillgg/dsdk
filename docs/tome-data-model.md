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
- **Basic tomes** — the header and one data tome per channel, rows tagged
  `match_key`, built in one go by `build_basic_tomes` (below).

Reading back:

- `get_dataframe`, `scan`, `get_keyset`, `get_manifest`, `iterate_pages`
- `get_match_by_index`, `get_random_match`

## Loading a tome

`get_dataframe(columns=None, library="pandas")` reads every page into one
frame; `columns` picks columns, in that order. A tome with no pages raises
`ValueError`.

- **pandas, the default.** One pyarrow dataset scan over the pages, then one
  conversion to pandas. The frame equals `pd.concat` of the pages read one by
  one with `pd.read_parquet`, as dsdk 3.2.2 and earlier returned it: the same
  values and dtypes, including the `Int64` pandas restores from a page's
  metadata, and `object` or `str` strings depending on the pandas version.
- **One index.** The frame has one `RangeIndex`, 0 to n - 1. dsdk 3.2.2 and
  earlier repeated each page's index, and pages written by dsdk 3.2.1 and
  earlier restored the index they stored (`__index_level_0__`). The old index
  is not kept as a column.
- **Pages that differ.** A column goes through the one scan when its Arrow
  type and pandas dtype agree on every page, and in the two drifts real tomes
  have:
  - an id column that some csds versions declare `int64` and others `Int64`:
    joined as `Int64`, as `pd.concat` does;
  - `player_id_fixed` and `attacker_id_fixed`, int64 on some pages and double
    on others: joined as `float64`, when the page footers' statistics show
    every int64 value within ±2^53, which a double holds exactly.

  Any other difference, a column some pages lack, and a `category` column on
  several pages are read page by page and joined with `pd.concat`, as before:
  pandas picks their dtype in ways pyarrow doesn't, and differently in
  pandas 2 and 3. The exception is a column some pages hold as int64 or
  float64 and others narrower, which is narrowed (next section).
- **polars.** `get_dataframe(library="polars")` returns a polars DataFrame,
  and `scan()` a LazyFrame that reads only the columns and rows a query
  needs. Both need the `polars` extra (`pureskillgg-dsdk[polars]`). Each page
  is scanned with `pl.scan_parquet`, old pages' index column is dropped, and
  the pages are joined with `pl.concat(how="diagonal_relaxed")`. polars keeps
  its own types: it has no `int64`/`Int64` split, strings are `String`, and a
  column int64 on some pages and double on others is `Float64`.

Warm loads, median of 3, on the maintainer's machine:

| tome | pandas | 3.2.2 | 3.3.0 | polars |
| --- | --- | --- | --- | --- |
| 9 gzip pages, 6.1M rows × 48 columns | 2.3.3 | 1.53 s | 0.77 s | 0.21 s |
| | 3.0.6 | 1.26 s | 0.45 s | 0.21 s |
| 9 zstd pages, 17.4M rows × 7 columns, `player_id_fixed` drifting | 2.3.3 | 1.25 s | 0.67 s | 0.08 s |
| | 3.0.6 | 0.80 s | 0.30 s | 0.08 s |

## Column types: old and compact csds

csgo-ppp's compact player tables (`player_vector` and `player_status`) store
integers as int8, int16 or int32, floats as float32 and `place_name` as a
dictionary, without pandas metadata. Files written before them store int64,
float64 and strings. Older files also hold some flags (`burst_mode` and
`is_silenced` in `player_status`, and flags in `player_death`, `other_death`
and `bomb_defuse`) as 0 and 1 in int64 columns declared `Int64`, where newer
ones hold bool. A tome can hold all of these, and loads them as follows.

- **A tome of compact files only** loads the narrow types: numpy `int8`,
  `int16`, `int32` and `float32` in pandas, `Int8` to `Float32` in polars.
- **A tome that mixes old and compact files is narrowed, not widened.**
  Where some matches or pages hold a column as int64 or float64 and others
  as a narrower type, the wide values are cast to the narrow type: integers
  to the widest narrow type seen, float64 to float32, and a float64 that
  holds whole numbers (`player_id_fixed` in some csds versions) to the
  integer type. Where some hold a column as bool and others as integers,
  the integers are cast to bool: 0 to False and 1 to True. Any other mix
  (unsigned types, an integer with float32, bool with a float) is joined as
  before.
- **Every cast is checked.** A value that doesn't fit raises
  `pureskillgg_dsdk.tome.NarrowingError`, which names the column, where the
  value is and the value: past the type's range or below it, a float that
  isn't a whole number (or is NaN or infinite) for an integer column, a
  finite float beyond float32's range, or a flag other than 0 or 1.
  `build_basic_tomes` raises it during the build, naming the match; the
  loaders raise it when they read a tome whose pages differ, naming the page
  and row, and the match when the page has a `match_key` column.
- **`current_ammo` stays int64** in a mixed tome, on every page: the compact
  files' int16 is widened to it, which is exact. Older files hold 4294967295
  there for an empty magazine, which int16 can't hold, and dsdk doesn't
  change values. A tome of compact files only loads it as int16.
- **Values don't change, except float32 rounding.** A float64 narrowed to
  float32 is rounded to the nearest float32. csgo-ppp's raw floats came from
  the demo as float32, so they convert back exactly; its derived columns
  (speeds, velocities, movement angles) were computed in float64, and move by
  at most about 6 parts in 100 million, as they do in the compact files.
- **pandas dtypes.** A narrowed column keeps the nullability its file
  declared: old files declare most integer columns `Int64`, so a mixed tome
  loads those as `Int8`, `Int16` or `Int32`, where a tome of compact files
  only loads numpy `int8`, `int16` or `int32`. An integer column with missing
  values, because some matches lack it (`player_controller_id` in some 2023
  files) or it holds nulls, is the nullable type of its width, not float64.
  A flag narrowed from a column declared `Int64`, or one with missing
  values, is the nullable `boolean`; a missing value stays missing. polars
  has no such split; its narrow columns hold nulls as they are.
- **Narrow integers can wrap in arithmetic.** pandas and polars keep int16 in
  element-wise arithmetic, so with int16 `money`, `money * 5` gives 14,464
  for 16,000. Sums and means widen and are safe. For analysis code,
  `get_dataframe(widen=True)` and `scan(widen=True)` return every integer
  column as a 64-bit integer (`Int16` becomes `Int64`) and every float
  column as float64, with the values of the default load. Flags stay bool
  (`bool` or `boolean`). The default load is unchanged for tomes of old
  files.
- **Where it happens.** `build_basic_tomes` narrows each page as it builds
  it, with pyarrow or, when pyarrow can't join a page, with pandas. When the
  tome is finished, every page not yet at the tome's types is cast to them
  and written again ("Page narrowed" in the log): a page written before the
  first compact match turned up, a page narrowed to int8 when a later match
  brought int16, and a page holding a value that int8 or int16 can't hold,
  which keeps that column wide until then, since a later match may bring a
  wider type. So every page holds the tome's types, and a value that
  doesn't fit them stops the build there. The loaders narrow across pages,
  for tomes whose pages differ, such as a `make_tome` tome continued after
  the format changed. `make_tome` joins each page's frames with `pd.concat` as before,
  so a page that mixes old and compact matches is widened there.
  `iterate_pages` reads each page as it is.

## Page storage

A tome is a folder: the JSON manifest `tome`, and two parquet files per page,
`dataframe_NNNNN` (the rows) and `keyset_NNNNN` (the match keys). A tome built
on a header also holds a copy of that header under `header/`.

- **Codec.** New pages are written with **zstd**. dsdk 3.2.2 and earlier wrote
  gzip, at pyarrow's default level 9, and that write was about 95% of the time
  to build a large tome: one 17.4M-row page took 119 s with gzip and 5 s with
  zstd, and came out 4% smaller.
- `make_tome`, `create_header_tome` and `create_subheader_tome` (and
  `TomeWriterFs`) take `compression=`, any codec `DataFrame.to_parquet`
  accepts: `"gzip"` for the old format, `None` for uncompressed pages. An
  unknown codec fails when the writer is created, not at the first page.
  `make_tome` writes the tome's header copy with the same codec.
- Each page file records its own codec, so readers need no setting. Old gzip
  tomes read unchanged, and a tome continued with a different codec (for
  example a gzip tome continued with the zstd default) reads back whole.
  pyarrow, polars and DuckDB all read zstd parquet.

## build_basic_tomes: the header and a tome per channel

`TomeCuratorFs.build_basic_tomes(channels)` builds what `create_header_tome`
and a loop of `make_tome` calls would, one per channel over a subheader of the
matches that have it, but visits each match once:

```python
built = curator.build_basic_tomes(["player_death", "round_end"], max_page_size_mb=256)
built.header                  # TomeLoader of the header tome
built.tomes["player_death"]   # TomeLoader of basic_player_death.<dates>
built.dates                   # "2023-11-10,2026-07-10": the header's match_date range
```

- **One walk.** For each match the builder reads the manifest, the `header`
  channel (when it builds the header) and every requested channel, with
  `pyarrow.parquet.read_table`. A loop of `make_tome` calls reads each
  manifest once per channel, plus once for the header and once to find which
  matches have which channels.
- **Names.** `tome_name` (default `basic_{channel}.{dates}`) names each
  channel's tome; `{dates}` is the first and last day of the header's
  `match_date`. `header_tome_name` defaults to the curator's default header.
  When the builder builds the header, the dates are known only once every
  match is read, so the channel tomes are written to a staging folder,
  `tome/<ds_type>/.building/<header name>/<channel>`, and moved to their
  names at the end (a rename on the same disk). Each gets its header copy
  before its manifest is marked complete.
- **Contents.** A channel's tome holds the matches that have the channel, in
  header order, and its header copy is their header rows. A match whose file
  is empty is in the keyset with no rows, as with `make_tome`. A channel no
  match has gets no tome. Only the header and the channel tomes are written;
  make other subheaders (all matches, a map, a platform) from the header with
  `create_subheader_tome`.
- **Keys.** `keys=` builds over those matches instead of every match in the
  collection. A key listed twice, or two manifests declaring the same key, is
  built once, and the count dropped is logged.
- **Pages.** A page is cut after the match that takes its tables past
  `max_page_size_mb` (default 256) of Arrow buffers, `Table.nbytes`. That
  counts a string by its bytes, where `make_tome` counts pandas' in-memory
  size, so builder pages hold more rows for the same setting. The header is
  one page, as `create_header_tome` writes it. Pages are zstd by default
  (`compression=`).
- **dtypes.** pyarrow joins a page's tables with
  `concat_tables(promote_options="permissive")`: int64 with double becomes
  double, null-typed columns take the other files' type, and missing columns
  are filled with nulls. The page's pandas metadata names the dtype
  `pd.concat` gives the same matches' frames, so `TomeLoader` restores what a
  `make_tome` page gives: a column is `Int64` when any match's file declares
  `Int64`, even though some csds versions declare the id columns `int64`.
  The builder works this out from two-row stand-ins of each distinct file
  schema, and checks that the joined page reads back with those dtypes. When
  it doesn't, or pyarrow can't join the tables, that page is built with
  `pd.concat`, as `make_tome` would, and the builder logs "Page built with
  pandas".
- **Categories.** A dictionary (category) column, such as `place_name` in
  the compact `player_status`, is decoded to its values as each match is
  read, so pyarrow joins matches whose dictionaries differ. pandas reads it
  as strings, which is what `pd.concat` gives categories that differ between
  matches; a `make_tome` page whose matches all share one set of categories
  keeps `category` instead.
- **Old and compact csds.** Where the matches mix int64 or float64 with
  narrower types, the builder narrows instead of widening, with checked
  casts (see "Column types: old and compact csds").
- **Threads.** `read_threads` (default 4) threads read whole matches ahead
  of the writer; results are used in key order, so pages come out the same
  for any count. 0 or 1 reads inline. On a USB hard disk 4 threads read
  about 1.5x faster than 1, and 8 no faster. Write the tomes to a different
  disk from the collection: writing to the disk being read slows the writes
  several times over.
- **Resuming.** A complete tome is kept (`behavior_if_complete="pass"`), so
  a call whose tomes are all complete reads no match; `"overwrite"` builds it
  again (for the header, by rescanning the collection) and `"fail"` raises.
  An interrupted build leaves no partial channel tome, only its staging
  folder and a partial header, and the next call starts over. A partial tome,
  such as one an interrupted `make_tome` left, is built again by default
  (`behavior_if_partial="overwrite"`), or left (`"pass"`), or raises
  (`"fail"`). There is no "continue": the build is one walk, so continuing
  one tome would mean reading every match again anyway.

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
- The size check is **in-memory** size (`memory_usage(deep=True)` of the
  page's columns, so string contents count), which runs 2-10x larger than the
  parquet on disk — budget pages accordingly.
- Each check measures only the frames added since the previous check, and
  adds them to a running total for the page, so frequent checks stay cheap.
  When the new frames add a column, lack one, or bring a different dtype,
  `pd.concat` fills or converts rows already measured (next point), so that
  check measures the whole page instead. The total equals the built page's
  deep size, less its index; a `category` column counts its categories once
  per check, a small overcount.
- A page is `pd.concat` of the frames passed to `concat`. A column keeps its
  dtype when every frame agrees on it; when frames disagree, pandas picks a
  common dtype (`category` columns with different category sets become
  `object`, `int64` with `float64` becomes `float64`). Frames with differing
  columns union (missing values are NA), and frames with no rows are skipped.
- `TomeManifest` builds the keyset and dataframe parquet keys and records
  per-page and total timings.
- An **empty tome is not supported** ("Empty Tome not supported"), and there is a
  non-obvious copied-header key path to be aware of when reading the code.
