# pureskillgg-dsdk, incremental tomes (local fork)

A local fork of [`pureskillgg/dsdk`](https://github.com/pureskillgg/dsdk) 4.1.1 (MIT) in which the **basic-tome builder is
iterative**: `TomeCuratorFs.build_basic_tomes` can **resume** an interrupted build and **extend** a finished tome collection with the
matches that appeared since, without starting over. Branch `incremental-tomes`, based on tag `v4.1.1` (commit `779a56c`).
Nothing here has been pushed or proposed upstream.

Everything is additive and off by default: with no new argument the builder behaves as in 4.1.1, and every upstream test passes.
The one change that is always on is that manifests and pages are written all or nothing (below).

## What changed

| File | Change |
| --- | --- |
| `tome/writer_fs.py` | Manifests and pages are written to `<file>.<pid>.tmp`, flushed (`fsync`) and renamed over the target (`os.replace`, retried briefly on the Windows "file in use" error). `write_atomically`, `replace_file`, `TomeWriterFs.write_parquet` are new. |
| `tome/incremental.py` (new) | Reads a tome's ledger from disk (`read_tome_state`), the `signatures` manifest field, `remove_orphans`. |
| `tome/builder.py` | `resume`, `behavior_if_complete="extend"`, the friendlier `keys` error; the scribe can continue a staged tome (`resume`) or extend a finished one (`extend`, `seed`). |
| `tome/curator.py` | `build_basic_tomes(..., resume=None, behavior_if_complete=None)`; the two options can also come from the environment. |
| `tome/*_test.py` | `incremental_test.py` (new) and additions to `writer_fs_test.py`. |

## The options

| Option | Default | Environment variable | Meaning |
| --- | --- | --- | --- |
| `resume` | `False` | `PURESKILLGG_TOME_RESUME` (`1`/`true`/`yes`/`on`, `0`/`false`/`no`/`off`) | Continue an interrupted build from its staging folder instead of deleting it. |
| `behavior_if_complete` | `"pass"` | `PURESKILLGG_TOME_BEHAVIOR_IF_COMPLETE` | `"pass"`, `"overwrite"`, `"fail"` as before, and the new `"extend"`. The argument wins over the variable. |

Call (`PURESKILLGG_TOME_*` paths as for any `TomeCuratorFs`):

```python
curator = TomeCuratorFs(default_header_name="header", ds_type="csds",
                        ds_collection_root_path=raw, tome_collection_root_path=out)
curator.build_basic_tomes(channels, tome_name="basic_{channel}",
                          behavior_if_complete="extend", resume=True)
```

That one call is a fresh build when nothing exists, a resumed build when a build was interrupted, an extension when a finished collection
exists and the collection has new matches, and a no-op (nothing read, nothing written) otherwise.

## The on-disk contract: manifest + pages + keysets are the ledger

A tome is a manifest (`tome/<ds_type>/<name>/tome`, JSON) and page files (`dataframe_NNNNN`, `keyset_NNNNN`, parquet). The manifest
lists the pages; a page's keyset lists the match keys whose rows the page holds. Both resume and extend rest on three rules, all of
which hold for tomes written by dsdk 4.1.1:

1. **A page is immutable once the manifest lists it** (except that the builder's narrowing step may replace a page by a whole file
   with the same rows at other types; see below).
2. **The manifest is the commit point**: a page file the manifest does not list does not exist as far as readers are concerned.
3. **The union of the keysets is what the tome holds**, and a channel tome lists matches in the order they were added.

Two additive manifest fields (readers ignore unknown fields):

* `signatures`: the distinct Arrow schemas (IPC, base64) of the matches in the tome. The narrowing plan of a tome (`narrowing.py`)
  depends on every schema that went in, but a page keeps only the schema the plan gave it: a page that joined an `int8` match and an
  `int64` match is `int8`, and nothing on it says that a later `int16` match must widen it to `int16` *because* of the `int64`
  match. Recovering the signatures from the pages (parquet schema + pandas metadata) is therefore only approximate, so the builder
  records them. (A tome without the field is read from its pages: exact when its columns never mixed wide and narrow types, otherwise
  a later match may widen fewer old pages. The values read are the same either way.)
* `resume` (staging manifests only): the columns read, the id of the tome being extended and the first page number, so a staged tome
  built for something else is started over.

`extend` also sets `extendedAt` on the manifests it replaces.

## Resume (`resume=True`)

The staging folder `tome/<ds_type>/.building/<header>/` holds a manifest and pages per channel; each manifest lists exactly the
pages whose files were written (the manifest is replaced after the pages). With `resume=True` the builder:

* keeps the staging folder, reloads each channel's staged manifest and the keysets of its pages as the channel's ledger, and checks
  that every listed page still has a readable parquet footer and keyset; the ledger is the leading run of pages that do (a page cut
  short by a crash, or one moved away by an interrupted publish, and everything after it, is read again);
* continues the page numbering, and restores the narrowing signatures from the manifest;
* walks the matches as always, but reads from each match only the channels whose tome lacks it (the header is written only at the
  end, so it is rebuilt: each match's manifest and header row are read; that is small);
* publishes exactly as before.

A staged channel built for other `columns`, or by dsdk 4.1.1 (no `signatures`), is started over; the others are not.
If a staged match is not found any more it raises (the pages would hold a match the header lacks). If matches came in that sort among
the staged ones, the tome lists the staged matches first, then the rest, and logs a warning (the rows are the same, the order is not
the header's).

Invariant, tested: *build(all matches) == build(first k matches, killed) then resume*, key for key, row for row, dtype for dtype.
Pages may be cut at other matches.

## Extend (`behavior_if_complete="extend"`)

For a **complete header** the builder takes its key set and keeps only the matches (from the collection, or from `keys`) that are not in
it; a match given twice, or declared by two manifests, is added once. Then:

* every channel tome that is complete gets **new pages** (numbers after its highest page) for the new matches only;
* the narrowing plan is worked out over the old **and** new signatures; an old page is **rewritten** only when the plan changes the type
  of one of its columns (for instance a new `int16` match widens an `int8` column that an old `int64` match had narrowed), as a whole
  file written beside it first, then renamed over the old page; a value that does not fit raises `NarrowingError` before anything in the
  collection has changed;
* the header is read, joined with the new rows as the builder joins any rows, and written again as page 0 (so tools that read
  `header/keyset_00000` keep working);
* the header copy (`<tome>/header/`) of every extended channel tome is regenerated for the union of keys, as a new page;
* a channel the collection has no tome for yet (or a partial one) is built over all matches, as in a fresh build;
* a call with nothing new reads no match and writes nothing; it removes the files a stopped extension left (below).

### Naming: no `{dates}`

`basic_{channel}.{dates}` takes `{dates}` from the header's `match_date` range, which changes when later matches arrive. The fork
**restricts extendable collections to a `tome_name` without `{dates}`** (for example `basic_{channel}`, which is what the project
driver uses); `behavior_if_complete="extend"` with a `{dates}` name raises at once and says so. Renaming tome folders and rewriting
`tome`/`key` in every manifest and page entry cannot be done atomically, and the readers find a tome by its name only, so it was not
done. `build_combined.py` finds tomes by the `basic_` prefix and takes the channel as the text up to the first dot, so
`basic_player_death` and `basic_player_death.2026-10-08,2026-10-12` are both read. Names without dates make dsdk's naming-convention
warning fire (`Header name of ... does not match convention`); the header already did that with the name `header`.

Order: the new matches follow the old ones in the header and in every channel tome. If they sort before the old ones, the order is not
the sorted order of a build of everything (the rows are the same).

### Crash safety

Nothing in a finished tome changes until the new pages, the new header and the narrowing have all been computed (that can fail, and the
collection is untouched when it does). Then, per extended channel tome, in this order:

1. the new page files are moved beside the old ones (not listed: orphans until step 4);
2. old pages that the narrowing changed are replaced by whole files of the same rows (each rename is atomic);
3. the header copy is written as a new page and its manifest replaced (atomically);
4. **the channel manifest is replaced (atomically): the new pages count from here**; the files it no longer lists are removed.

After all channels, the header: page 0's rows file is replaced, then its keyset file (the list of matches), then the manifest.

The channels come **before** the header because the header is the list of matches: a build that stops after some channels committed
finds the new matches still missing from the header, adds them again to the channels that lack them (the channels that hold them take
nothing twice: their keysets are the ledger) and then to the header. Stopping at any point therefore leaves every tome readable and
whole (`isComplete` is never set to `False` on an extended tome); the states a stop can leave, and what the next call does:

| Stopped | State | Next call |
| --- | --- | --- |
| while reading, or while computing the plan | collection as it was; staging folder | `extend` again (or `resume=True` to keep the staged pages) |
| between moving pages and the channel manifest | collection as it was, plus unlisted page files | removes them, builds again |
| after some channel manifests | those channels hold the new matches, the header does not | adds the new matches to the header and the other channels |
| while replacing the header | header rows may include the new matches while its keyset does not | rows not in the keyset are dropped, the new matches are read again |
| after the header | complete; unlisted files and staging may remain | removes them |

The header copy is written before the channel manifest, so after a stop it may list more matches than the tome, never fewer.
Unlisted page files are a hazard only for tools that list page files by glob instead of reading the manifest (`build_combined.py` does):
use the collection only when the run that extends it has finished (the project driver writes `BUILD_OK` last).

## Known limits

* One build per collection at a time (nothing locks the folders; the project driver takes a lock file).
* `resume` and `extend` cover the basic-tome builder (`build_basic_tomes`) only, not `make_tome`.
* A page counts as intact if its parquet footer reads (a truncated file does not); the pages are fsynced, but a page whose bytes are
  damaged elsewhere is not detected.
* Extending a tome that dsdk 4.1.1 wrote works, but its narrowing is recovered from its pages (see `signatures`).
* `extend` never removes a match; a match removed from the collection stays in the tomes. `keys` can only add.
* Nothing is stored about channels that no match has; a call that names a channel with no tome rebuilds it over all matches each time
  (the project driver remembers such channels).
* The matches of a channel tome are listed in the order added, so after an extension it is not sorted by key.
