"""
Read a tome's dataframe pages as one frame: pandas through a pyarrow dataset,
or polars.

The pandas frame equals ``pd.concat`` of every page read with
``pd.read_parquet``, as dsdk 3.2.2 and earlier returned it, except for the
index, which is one ``RangeIndex``. A column is read with one pyarrow dataset
scan and converted once when its type is the same on every page, or in the
two drifts real tomes have: int64 on some pages and Int64 on others, and
int64 on some pages and double on others. Any other column whose type
differs between pages, or that some pages lack, is read page by page and
joined with ``pd.concat``, as before, because pandas picks its dtype in ways
pyarrow doesn't (and differently in pandas 2 and 3).
"""

import json
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, List, Optional, Sequence

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

if TYPE_CHECKING:
    import polars as pl

_PANDAS_METADATA = b"pandas"
_PANDAS_ATTRS = b"PANDAS_ATTRS"
_PYARROW_MAJOR = int(pa.__version__.split(".", maxsplit=1)[0])
# Every integer from -2**53 to 2**53 converts to a double exactly.
_DOUBLE_EXACT = 2**53

POLARS_MISSING_MESSAGE = (
    "Reading tomes with polars needs polars, which dsdk installs only with its"
    ' polars extra: pip install "pureskillgg-dsdk[polars]"'
)


@dataclass(frozen=True)
class PageInfo:
    """What a page's parquet footer says about its data columns."""

    path: str
    num_rows: int
    # Data columns in file order; the pandas index columns are left out.
    fields: dict
    index_columns: List[str]
    # pandas dtype and pandas metadata entry of each data column.
    dtypes: dict
    columns_metadata: dict
    pandas_metadata: Optional[dict]
    attrs: Optional[bytes]
    # int64 columns whose values all convert to double exactly.
    exact_in_double: frozenset

    @property
    def is_empty(self) -> bool:
        """No rows and no columns: pd.concat leaves such a frame out."""
        return self.num_rows == 0 and len(self.fields) == 0


def read_page_info(path: str) -> PageInfo:
    with pq.ParquetFile(path) as parquet_file:
        schema = parquet_file.schema_arrow
        file_metadata = parquet_file.metadata
    metadata = schema.metadata or {}
    pandas_metadata = None
    if _PANDAS_METADATA in metadata:
        pandas_metadata = json.loads(metadata[_PANDAS_METADATA])
    index_columns = [
        name
        for name in (pandas_metadata or {}).get("index_columns", [])
        if isinstance(name, str)
    ]
    fields = {field.name: field for field in schema if field.name not in index_columns}
    entries = {
        entry.get("field_name", entry["name"]): entry
        for entry in (pandas_metadata or {}).get("columns", [])
    }
    # The dtypes pd.read_parquet gives this page, from the footer alone. The
    # frame's columns are the data columns, in order.
    empty = schema.empty_table().to_pandas(types_mapper=_types_mapper())
    return PageInfo(
        path=path,
        num_rows=file_metadata.num_rows,
        fields=fields,
        index_columns=index_columns,
        dtypes=dict(zip(fields, empty.dtypes, strict=True)),
        columns_metadata={name: entries[name] for name in fields if name in entries},
        pandas_metadata=pandas_metadata,
        attrs=metadata.get(_PANDAS_ATTRS),
        exact_in_double=_int64_columns_exact_in_double(file_metadata, fields),
    )


def _int64_columns_exact_in_double(file_metadata, fields):
    """
    The int64 columns whose row-group statistics put every value within
    +/- 2**53, the integers a double holds exactly. Without statistics, a
    column is left out.
    """
    positions = {
        file_metadata.schema.column(i).path: i for i in range(file_metadata.num_columns)
    }
    exact = set()
    for name, field in fields.items():
        if field.type != pa.int64() or name not in positions:
            continue
        row_groups = (
            file_metadata.row_group(i) for i in range(file_metadata.num_row_groups)
        )
        if all(_exact_in_double(group, positions[name]) for group in row_groups):
            exact.add(name)
    return frozenset(exact)


def _exact_in_double(row_group, position):
    if row_group.num_rows == 0:
        return True
    stats = row_group.column(position).statistics
    if stats is None:
        return False
    if not stats.has_min_max:
        # A column chunk of nulls only has no min or max.
        return stats.has_null_count and stats.null_count == row_group.num_rows
    return -_DOUBLE_EXACT <= stats.min and stats.max <= _DOUBLE_EXACT


def select_columns(pages: Iterable[PageInfo], columns: Optional[Sequence[str]]):
    """The columns to read: every data column in page order, or `columns`."""
    available = list(dict.fromkeys(name for page in pages for name in page.fields))
    if columns is None:
        return available
    if isinstance(columns, str):
        raise TypeError("columns must be a list of column names, not a string")
    names = list(dict.fromkeys(columns))
    known = set(available)
    missing = [name for name in names if name not in known]
    if missing:
        raise KeyError(f"Columns not in the tome: {missing}")
    return names


def read_pages_pandas(
    paths: Sequence[str], columns: Optional[Sequence[str]] = None
) -> pd.DataFrame:
    """Read the pages at `paths` into one pandas frame with a RangeIndex."""
    pages = [read_page_info(path) for path in paths]
    names = select_columns(pages, columns)
    live = [page for page in pages if not page.is_empty]

    steady = {}
    for name in names:
        plan = _plan_steady_column(name, live)
        if plan is not None:
            steady[name] = plan
    drifting = [name for name in names if name not in steady]

    if drifting and not steady:
        frame = _read_page_by_page(pages, drifting)[drifting]
    else:
        frame = _read_steady_columns(live, steady)
        if drifting:
            drift_frame = _read_page_by_page(pages, drifting)
            for position, name in enumerate(names):
                if name in drifting:
                    frame.insert(position, name, drift_frame[name])
    frame.attrs = _concat_attrs(live)
    return frame


def _plan_steady_column(name, pages):
    """
    The field and pandas metadata entry to read `name` in one dataset scan, or
    None when the column must be read page by page to keep today's dtype.
    """
    if len(pages) == 0 or any(name not in page.fields for page in pages):
        return None
    dtype_pages = _pages_giving_the_dtype(name, pages)
    if dtype_pages is None:
        return None
    entries = [page.columns_metadata.get(name) for page in dtype_pages]
    return (
        _unify_fields([page.fields[name] for page in pages]),
        next((entry for entry in entries if entry), None),
    )


def _pages_giving_the_dtype(name, pages):
    """
    When pyarrow's promotion joins the pages' `name` columns into the dtype
    pd.concat gives them, the pages whose metadata entry restores that dtype.
    Otherwise None.
    """
    fields = [page.fields[name] for page in pages]
    dtypes = [page.dtypes[name] for page in pages]
    if _int64_and_float64_promote_exactly(name, pages):
        # player_id_fixed and attacker_id_fixed are int64 in some csds
        # versions and double in others. pd.concat gives float64, and so does
        # pyarrow's promotion of int64 to double.
        return [page for page in pages if page.dtypes[name] == np.dtype("float64")]
    if not _same_arrow_type(fields):
        return None
    if all(dtype == dtypes[0] for dtype in dtypes):
        # Categories live in the data, not the footer, and pd.concat turns
        # differing ones into strings, so only one page keeps them.
        several_categoricals = (
            isinstance(dtypes[0], pd.CategoricalDtype) and len(pages) > 1
        )
        return None if several_categoricals else pages
    if pa.types.is_integer(fields[0].type) and _is_int_and_nullable_int(dtypes):
        # pd.concat of int64 and Int64 pages gives Int64. Some csds versions
        # declare an id column int64 and others Int64, so tomes mix them. A
        # nullable dtype only comes from a page's metadata entry.
        return [
            page
            for page in pages
            if isinstance(page.dtypes[name], pd.api.extensions.ExtensionDtype)
        ]
    return None


def _same_arrow_type(fields):
    def normal(data_type):
        return pa.string() if data_type == pa.large_string() else data_type

    first = normal(fields[0].type)
    return all(normal(field.type) == first for field in fields)


def _int64_and_float64_promote_exactly(name, pages):
    """
    The column is int64 on some pages and double on the others, as numpy
    dtypes, and every int64 value converts to double exactly: pyarrow refuses
    to promote larger ones, where pd.concat rounds them.
    """
    arrow_types = {page.fields[name].type for page in pages}
    if arrow_types != {pa.int64(), pa.float64()}:
        return False
    for page in pages:
        if page.dtypes[name] not in (np.dtype("int64"), np.dtype("float64")):
            return False
        if page.fields[name].type == pa.int64() and name not in page.exact_in_double:
            return False
    return True


def _unify_fields(fields):
    schema = pa.unify_schemas(
        [pa.schema([field]) for field in fields], promote_options="permissive"
    )
    return schema.field(0)


def _is_int_and_nullable_int(dtypes):
    numpy_dtypes = set()
    for dtype in dtypes:
        if isinstance(dtype, pd.api.extensions.ExtensionDtype):
            if not pd.api.types.is_integer_dtype(dtype):
                return False
            numpy_dtypes.add(np.dtype(dtype.numpy_dtype))
        elif pd.api.types.is_integer_dtype(dtype):
            numpy_dtypes.add(np.dtype(dtype))
        else:
            return False
    return len(numpy_dtypes) == 1


def _read_steady_columns(pages, steady) -> pd.DataFrame:
    rows = sum(page.num_rows for page in pages)
    if len(steady) == 0:
        return pd.DataFrame(index=pd.RangeIndex(rows))
    schema = pa.schema([field for field, _ in steady.values()])
    dataset = ds.dataset(
        [os.path.abspath(page.path) for page in pages], schema=schema, format="parquet"
    )
    table = dataset.to_table()
    pandas_metadata = _merged_pandas_metadata(pages, steady)
    if pandas_metadata is not None:
        table = table.replace_schema_metadata(
            {_PANDAS_METADATA: json.dumps(pandas_metadata).encode("utf-8")}
        )
    else:
        table = table.replace_schema_metadata(None)
    return table.to_pandas(types_mapper=_types_mapper())


def _merged_pandas_metadata(pages, steady):
    """
    pandas metadata for the steady columns: no index columns, so to_pandas
    gives a RangeIndex, and each column's entry from a page that has one.
    """
    base = next(
        (page.pandas_metadata for page in pages if page.pandas_metadata is not None),
        None,
    )
    if base is None:
        return None
    column_indexes = next(
        (
            page.pandas_metadata.get("column_indexes")
            for page in pages
            if page.pandas_metadata is not None and len(page.fields) > 0
        ),
        base.get("column_indexes", []),
    )
    merged = dict(base)
    merged["index_columns"] = []
    merged["column_indexes"] = column_indexes
    merged["columns"] = [entry for _, entry in steady.values() if entry is not None]
    return merged


def _read_page_by_page(pages, names) -> pd.DataFrame:
    """Today's read, for `names` only: pd.read_parquet each page, then pd.concat."""
    frames = []
    for page in pages:
        page_columns = [name for name in names if name in page.fields]
        if page_columns:
            frames.append(pd.read_parquet(page.path, columns=page_columns))
        else:
            frames.append(pd.DataFrame(index=pd.RangeIndex(page.num_rows)))
    return pd.concat(frames, ignore_index=True)


def _concat_attrs(pages):
    """
    pd.concat keeps `attrs` only when every page has equal, non-empty ones. It
    leaves out pages with no rows and no columns, so the caller does too.
    """
    attrs = [json.loads(page.attrs) if page.attrs else {} for page in pages]
    if len(attrs) == 0 or not all(attrs) or any(value != attrs[0] for value in attrs):
        return {}
    return attrs[0]


def _types_mapper():
    """
    Map string columns the way pd.read_parquet does. pandas 3 reads them as
    its `str` dtype: pyarrow 19 and later do that in to_pandas, and pandas
    passes this mapping to earlier pyarrow.
    """
    if _PYARROW_MAJOR >= 19 or not pd.get_option("future.infer_string"):
        return None
    dtype = pd.StringDtype(na_value=np.nan)
    return {pa.string(): dtype, pa.large_string(): dtype}.get


def import_polars():
    try:
        # pylint: disable-next=import-outside-toplevel
        import polars
    except ImportError as err:
        raise ImportError(POLARS_MISSING_MESSAGE) from err
    return polars


def scan_pages_polars(
    paths: Sequence[str], columns: Optional[Sequence[str]] = None
) -> "pl.LazyFrame":
    """
    Scan the pages at `paths` as one polars LazyFrame. The pandas index
    columns of old pages are dropped, and pages whose schemas drift are
    joined with ``how="diagonal_relaxed"``.
    """
    polars = import_polars()
    pages = [read_page_info(path) for path in paths]
    names = select_columns(pages, columns)
    frames = []
    for page in pages:
        if page.is_empty:
            continue
        frame = polars.scan_parquet(page.path, hive_partitioning=False, glob=False)
        if page.index_columns:
            frame = frame.drop(page.index_columns)
        frames.append(frame)
    if len(frames) == 0:
        return polars.LazyFrame()
    lazy = polars.concat(frames, how="diagonal_relaxed")
    if columns is not None:
        lazy = lazy.select(names)
    return lazy
