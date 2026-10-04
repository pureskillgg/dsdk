"""
Join tables whose columns are 64-bit in some and narrower in others at the
narrower type.

CSDS files written before csgo-ppp's compact player tables hold every integer
as int64 and every float as float64. Newer files hold the player tables'
integers as int8, int16 or int32 and their floats as float32. When the tables
a tome joins mix the two, the wide columns are narrowed to the narrow files'
types, instead of the narrow ones widened. Every cast is checked: a value the
narrow type can't hold raises `NarrowingError`, naming the column, where the
value is and the value. Values never change, except that a float64 narrowed
to float32 is rounded to the nearest float32, as the narrow files' floats
were.

The rules, for one column across the tables or pages being joined:

- Signed integers of 8, 16 or 32 bits together with int64 or float64: the
  64-bit ones are narrowed to the widest of the narrow types. A float64 must
  hold whole numbers to be narrowed (``player_id_fixed`` was float64 in some
  csds versions).
- float32 together with float64: the float64 ones are narrowed to float32. A
  finite value beyond float32's range is an overflow.
- bool together with integers: the integers are narrowed to bool. Only 0 and
  1 may be narrowed, to False and True. Older csds files hold flags such as
  ``burst_mode`` as 0 and 1 in int64 columns declared ``Int64``; newer ones
  hold bool.
- Any other mix (or a column in `KEEP_WIDE`) is joined as before.

A pandas metadata entry follows its column: a nullable ``Int64`` becomes the
nullable type of the narrow width (``boolean`` for a flag), so pandas gives
what ``pd.concat`` gives, at the narrow width. An integer column that ends up
with missing values, because some tables lack it or it holds nulls, is the
nullable ``Int8``, ``Int16`` or ``Int32`` in pandas, rather than float64; a
narrowed flag is the nullable ``boolean``.
"""

# pyarrow.compute builds most of its functions when it is imported, so pylint
# can't see them.
# pylint: disable=no-member

import json

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

# Never narrowed. current_ammo is int16 in the new files, but older files
# hold 4294967295 for an empty magazine, which int16 can't hold; dsdk doesn't
# change values, so a mixed tome keeps it int64, on every page.
KEEP_WIDE = frozenset({"current_ammo"})

NARROW_INTS = (pa.int8(), pa.int16(), pa.int32())
_WIDE = (pa.int64(), pa.float64())
_PANDAS_METADATA = b"pandas"
_TYPE_NAMES = {pa.float32(): "float32", pa.float64(): "float64"}


class NarrowingError(ValueError):
    """A value doesn't fit the type its column is narrowed to."""

    def __init__(self, message, *, column=None, value=None):
        super().__init__(message)
        self.column = column
        self.value = value


def type_name(data_type) -> str:
    """The numpy-style name of an Arrow type: float32, not float."""
    return _TYPE_NAMES.get(data_type, str(data_type))


def narrow_target(name, types):
    """
    The type to narrow column `name` to, given its types in the tables being
    joined, or None when it is joined as before. Null-typed columns (all
    missing) don't count. A column in `KEEP_WIDE` that is int64 in some
    tables and a narrower integer in others gets int64 instead: the narrow
    ones are widened, exactly, so every page holds the same type.
    """
    present = {data_type for data_type in types if not pa.types.is_null(data_type)}
    if name in KEEP_WIDE:
        mixed = pa.int64() in present and len(present) > 1
        if mixed and all(pa.types.is_signed_integer(t) for t in present):
            return pa.int64()
        return None
    narrow = [data_type for data_type in present if data_type in NARROW_INTS]
    target = None
    if pa.bool_() in present:
        others = present.difference([pa.bool_()])
        if others and all(pa.types.is_signed_integer(t) for t in others):
            target = pa.bool_()
    elif narrow:
        wide = present.difference(narrow)
        if wide and all(data_type in _WIDE for data_type in wide):
            target = max(narrow, key=lambda data_type: data_type.bit_width)
    elif present == {pa.float32(), pa.float64()}:
        target = pa.float32()
    return target


def plan_narrowing(schemas) -> dict:
    """Column name -> type to narrow it to, for the columns that need it."""
    types = {}
    for schema in schemas:
        for field in schema:
            types.setdefault(field.name, set()).add(field.type)
    plan = {}
    for name, column_types in types.items():
        target = narrow_target(name, column_types)
        if target is not None:
            plan[name] = target
    return plan


def narrow_table(table, plan, describe=None, *, check=True):
    """
    `table` with each column named in `plan` cast to its type, and its pandas
    metadata entry to match: a wide column is narrowed, and a narrower
    integer column (int8 where the plan says int16) is widened to it, which
    is exact. A null-typed column is left for the join to fill.

    `describe(row)` names where a row is, for the error. With ``check=False``
    the casts aren't checked; that is for two-row stand-ins whose values
    don't matter.
    """
    changes = {}
    for name, target in plan.items():
        if name not in table.column_names:
            continue
        position = table.column_names.index(name)
        column = table.column(position)
        if column.type == target or pa.types.is_null(column.type):
            continue
        if check:
            narrowed = cast_checked(column, target, name, describe)
        else:
            narrowed = column.cast(target, safe=False)
        table = table.set_column(position, table.schema.field(position).name, narrowed)
        changes[name] = (target, narrowed.null_count > 0)
    if not changes:
        return table
    return table.replace_schema_metadata(
        narrowed_metadata(table.schema.metadata, changes)
    )


def cast_checked(column, target, name, describe=None):
    """Cast a column to `target`, or raise NarrowingError for a value it can't hold."""
    if pa.types.is_boolean(target):
        # Only 0 and 1 are flags; a null stays null.
        ok = pc.or_(pc.equal(column, 0), pc.equal(column, 1))
        _raise_at_first(pc.invert(ok), column, target, name, describe)
        return pc.not_equal(column, 0)
    if pa.types.is_floating(target):
        narrowed = column.cast(target, safe=False)
        bad = pc.and_(pc.is_finite(column), pc.invert(pc.is_finite(narrowed)))
        _raise_at_first(bad, column, target, name, describe)
        return narrowed
    if (
        pa.types.is_signed_integer(column.type)
        and column.type.bit_width <= target.bit_width
    ):
        # A narrower signed integer widens exactly.
        return column.cast(target)
    info = np.iinfo(target.to_pandas_dtype())
    if pa.types.is_floating(column.type):
        # A float must hold a whole number in range. A NaN or an infinity
        # isn't one; a null stays null.
        ok = pc.and_(pc.is_finite(column), pc.equal(pc.floor(column), column))
        ok = pc.and_(ok, _in_range(column, info))
    else:
        ok = _in_range(column, info)
    _raise_at_first(pc.invert(ok), column, target, name, describe)
    return column.cast(target)


def _in_range(column, info):
    return pc.and_(
        pc.greater_equal(column, pa.scalar(info.min, column.type)),
        pc.less_equal(column, pa.scalar(info.max, column.type)),
    )


def _raise_at_first(bad, column, target, name, describe):
    if not pc.any(bad).as_py():
        return
    row = pc.index(bad, True).as_py()
    value = column[row].as_py()
    where = describe(row) if describe is not None else f"row {row}"
    target_name = type_name(target)
    raise NarrowingError(
        f"Column {name!r} of {where} holds {value!r}, which {target_name} can't"
        f" hold. Other tables store {name!r} as {target_name}, so the tome"
        f" narrows it to that type.",
        column=name,
        value=value,
    )


def narrowed_metadata(metadata, changes):
    """Schema metadata with the pandas entries of the narrowed columns updated."""
    if not metadata or _PANDAS_METADATA not in metadata:
        return metadata
    pandas_metadata = json.loads(metadata[_PANDAS_METADATA])
    for entry in pandas_metadata.get("columns", []):
        name = entry.get("field_name", entry.get("name"))
        if name not in changes:
            continue
        target, has_nulls = changes[name]
        entry["pandas_type"] = type_name(target)
        entry["numpy_type"] = narrow_numpy_type(
            entry.get("numpy_type"), target, has_nulls
        )
    updated = dict(metadata)
    updated[_PANDAS_METADATA] = json.dumps(pandas_metadata).encode("utf8")
    return updated


def narrow_numpy_type(numpy_type, target, has_nulls) -> str:
    """
    The pandas dtype name for a column narrowed to `target`: nullable when it
    was (Int64, Float64) or now holds nulls (a float64 with NaN, for example).
    """
    was_nullable = isinstance(numpy_type, str) and numpy_type[:1] in ("I", "U", "F")
    if pa.types.is_floating(target):
        return "Float32" if was_nullable else "float32"
    if pa.types.is_boolean(target):
        return "boolean" if was_nullable or has_nulls else "bool"
    bits = target.bit_width
    return f"Int{bits}" if was_nullable or has_nulls else f"int{bits}"


def resolved_narrow_int(types):
    """
    The narrow integer type a column joins to when every table holding it
    holds a narrow signed integer (after narrowing), or None.
    """
    present = [data_type for data_type in types if not pa.types.is_null(data_type)]
    if not present or not all(data_type in NARROW_INTS for data_type in present):
        return None
    return max(present, key=lambda data_type: data_type.bit_width)


def nullable_int_dtype(data_type):
    """pandas' nullable integer dtype for a narrow Arrow integer type."""
    return pd.api.types.pandas_dtype(f"Int{data_type.bit_width}")


def missing_as_nullable(dtypes, narrow_ints, flags=()) -> dict:
    """
    The columns pandas would read as float64 (or object) only because their
    narrow integers (or narrowed flags) have missing values, with the
    nullable dtype that holds them. ``narrow_ints`` maps column names to
    their resolved narrow integer type; ``flags`` names the columns narrowed
    to bool.
    """
    nullable = {
        name: nullable_int_dtype(data_type)
        for name, data_type in narrow_ints.items()
        if name in dtypes and dtypes[name] == np.dtype("float64")
    }
    for name in flags:
        if name in dtypes and dtypes[name] == np.dtype("object"):
            nullable[name] = pd.BooleanDtype()
    return nullable


def flag_columns(plan) -> list:
    """The columns a narrowing plan turns into bool."""
    return [name for name, target in plan.items() if pa.types.is_boolean(target)]


def widen_dtype(dtype):
    """
    The 64-bit dtype for widening on load, or None to leave the column as it
    is: integers to int64, floats to float64, keeping nullability.
    """
    wide = None
    if isinstance(dtype, pd.api.extensions.ExtensionDtype):
        if pd.api.types.is_integer_dtype(dtype):
            wide = pd.Int64Dtype()
        elif pd.api.types.is_float_dtype(dtype):
            wide = pd.Float64Dtype()
        # A 64-bit one stays, and UInt64 has no wider signed type.
        if dtype in (pd.Int64Dtype(), pd.UInt64Dtype(), pd.Float64Dtype()):
            wide = None
    elif isinstance(dtype, np.dtype) and dtype.itemsize < 8:
        wide = {"i": np.dtype("int64"), "u": np.dtype("int64")}.get(dtype.kind)
        if dtype.kind == "f":
            wide = np.dtype("float64")
    return wide


def widen_frame(frame) -> pd.DataFrame:
    """Widen a pandas frame's narrow integer and float columns in place."""
    for name, dtype in list(frame.dtypes.items()):
        wide = widen_dtype(dtype)
        if wide is not None:
            frame[name] = frame[name].astype(wide)
    return frame
