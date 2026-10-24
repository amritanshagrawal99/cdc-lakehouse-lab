"""Prove the lake table equals the source table: same keys, same values, column by column.

Only meaningful once the consumer has caught up with a quiesced source (no writes in flight); `wait_caught_up`
in the CLI does that by waiting until the lake's stored offsets reach the end of each topic.
"""
from __future__ import annotations

import datetime as dt
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal


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
    if isinstance(v, Decimal):
        return v.normalize() if v == v.to_integral_value() else v.quantize(Decimal(1).scaleb(v.as_tuple().exponent))
    if isinstance(v, dt.datetime) and v.tzinfo is not None:
        return v.astimezone(dt.timezone.utc)
    if isinstance(v, float):
        return round(v, 9)
    return v


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


def source_rows(conn, table: str) -> tuple[list[str], list[dict]]:
    cur = conn.execute(f"SELECT * FROM {table}")
    names = [c.name for c in cur.description]
    return names, [dict(zip(names, r, strict=True)) for r in cur.fetchall()]


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
