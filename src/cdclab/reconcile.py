"""Prove the lake table equals the source table: same keys, same values, column by column.

Only meaningful once the consumer has caught up with a quiesced source (no writes in flight); `wait_caught_up`
in the CLI does that by waiting until the lake's stored offsets reach the end of each topic.
"""
from __future__ import annotations

import datetime as dt
import math
import struct
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal

FLOAT4, FLOAT4_ARRAY = 700, 1021   # Postgres type OIDs


@dataclass
class Diff:
    table: str
    source_rows: int
    lake_rows: int
    missing_in_lake: list[tuple] = field(default_factory=list)
    extra_in_lake: list[tuple] = field(default_factory=list)
    changed: dict[tuple, dict[str, tuple]] = field(default_factory=dict)  # key -> column -> (source, lake)
    duplicate_in_lake: dict[tuple, int] = field(default_factory=dict)    # key -> copies (a key must appear once)

    @property
    def match(self) -> bool:
        return not (self.missing_in_lake or self.extra_in_lake or self.changed or self.duplicate_in_lake)

    def describe(self, limit: int = 5) -> str:
        if self.match:
            return f"MATCH  {self.table}: {self.source_rows} rows"
        parts = [f"MISMATCH {self.table}: source {self.source_rows} rows, lake {self.lake_rows}"]
        if self.missing_in_lake:
            parts.append(f"  missing in lake: {self.missing_in_lake[:limit]}{' ...' if len(self.missing_in_lake) > limit else ''}")
        if self.extra_in_lake:
            parts.append(f"  extra in lake: {self.extra_in_lake[:limit]}{' ...' if len(self.extra_in_lake) > limit else ''}")
        if self.duplicate_in_lake:
            parts.append(f"  duplicate keys in lake: {dict(list(self.duplicate_in_lake.items())[:limit])}")
        for k, cols in list(self.changed.items())[:limit]:
            parts.append(f"  changed {k}: " + "; ".join(f"{c}: {a!r} -> {b!r}" for c, (a, b) in cols.items()))
        return "\n".join(parts)


def normalise(v):
    """Make equal values compare equal. Comparison is otherwise EXACT: Decimal equality already ignores trailing
    zeros, and source floats are loaded at the precision the lake stores (see source_rows), so no tolerance is
    needed and none can hide a real difference."""
    if isinstance(v, Decimal):
        return v if v.is_finite() else ("non-finite", str(v))   # NaN != NaN, and NaN breaks quantize
    if isinstance(v, float):
        return ("non-finite", "NaN") if math.isnan(v) else v
    if isinstance(v, dt.datetime) and v.tzinfo is not None:
        return v.astimezone(dt.timezone.utc)
    if isinstance(v, list):
        return [normalise(x) for x in v]
    return v


def _float32(v):
    """A Postgres real at the precision Iceberg stores it (float32), as the double Python reads back."""
    if v is None:
        return None
    if isinstance(v, list):
        return [_float32(x) for x in v]
    return struct.unpack("f", struct.pack("f", v))[0]


def compare(table: str, key: list[str], source_rows: list[dict], lake_rows: list[dict], columns: list[str] | None = None) -> Diff:
    def index(rows):
        return {tuple(r[k] for k in key): r for r in rows}

    src, lake = index(source_rows), index(lake_rows)
    cols = columns or sorted(set().union(*(r.keys() for r in source_rows)) if source_rows else set())
    d = Diff(table, len(source_rows), len(lake_rows))
    counts = Counter(tuple(r[k] for k in key) for r in lake_rows)
    d.duplicate_in_lake = {k: n for k, n in counts.items() if n > 1}
    d.missing_in_lake = sorted(set(src) - set(lake))
    d.extra_in_lake = sorted(set(lake) - set(src))
    for k in sorted(set(src) & set(lake)):
        diffs = {c: (src[k].get(c), lake[k].get(c)) for c in cols
                 if normalise(src[k].get(c)) != normalise(lake[k].get(c))}
        if diffs:
            d.changed[k] = diffs
    return d


def _source_loaders(cur) -> None:
    """Load source values in the form Debezium sends them to the lake:
    * json / jsonb / uuid as text (Debezium sends Postgres' own text form; psycopg would parse them);
    * timestamp / timestamptz / date infinity as the max / min Python value (as connect_schema decodes them;
      psycopg would raise)."""
    from psycopg.types.datetime import DateLoader, TimestampLoader, TimestamptzLoader
    from psycopg.types.string import TextLoader

    def infinite(base, pos, neg):
        class Loader(base):
            def load(self, data):
                raw = bytes(data)
                if raw == b"infinity":
                    return pos
                if raw == b"-infinity":
                    return neg
                return super().load(data)
        return Loader

    utc = dt.timezone.utc
    for name in ("json", "jsonb", "uuid"):
        cur.adapters.register_loader(name, TextLoader)
    cur.adapters.register_loader("timestamp", infinite(TimestampLoader, dt.datetime.max, dt.datetime.min))
    cur.adapters.register_loader("timestamptz", infinite(TimestamptzLoader, dt.datetime.max.replace(tzinfo=utc),
                                                         dt.datetime.min.replace(tzinfo=utc)))
    cur.adapters.register_loader("date", infinite(DateLoader, dt.date.max, dt.date.min))


def source_rows(conn, table: str) -> tuple[list[str], list[dict]]:
    cur = conn.cursor()
    _source_loaders(cur)
    cur.execute(f"SELECT * FROM {table}")
    names = [c.name for c in cur.description]
    real = {c.name for c in cur.description if c.type_code in (FLOAT4, FLOAT4_ARRAY)}
    rows = [dict(zip(names, r, strict=True)) for r in cur.fetchall()]
    for r in rows:
        for n in real:
            r[n] = _float32(r[n])
    return names, rows


def primary_key(conn, table: str) -> list[str]:
    schema, name = table.split(".", 1)
    rows = conn.execute(
        """SELECT a.attname FROM pg_index i
           JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
           JOIN pg_class c ON c.oid = i.indrelid JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE i.indisprimary AND n.nspname = %s AND c.relname = %s ORDER BY array_position(i.indkey, a.attnum)""",
        (schema, name)).fetchall()
    return [r[0] for r in rows]


def lake_rows(iceberg_table) -> list[dict]:
    return iceberg_table.scan().to_arrow().to_pylist()
