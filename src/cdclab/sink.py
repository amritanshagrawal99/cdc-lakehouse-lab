"""Apply compacted CDC batches to Iceberg tables with exactly-once effect.

Each batch for a table becomes ONE Iceberg transaction: delete every key the batch touches, append the final rows,
and record the Kafka offsets the batch covers as table properties. Offsets therefore commit atomically with the data;
after a crash the consumer reads them back and resumes right after them.

Why delete + append rather than pyiceberg's upsert (all verified on pyiceberg 0.12):
  * upsert fails for keys that already exist once a column has been added (apache/iceberg-python#3105);
  * upsert builds one In() predicate, and pyiceberg stops pruning files for In() lists over 200 values;
  * upsert's row comparison is a Python loop, and NaN != NaN makes rows "change" on every replay.
Delete + append has none of these, at the cost of rewriting unchanged rows (both are copy-on-write in 0.12).

Tables are created with commit.retry.num-retries=0: pyiceberg's built-in retry would replay a stale plan (and stale
offsets) on top of a concurrent commit. The caller refreshes, re-checks offsets and recomputes instead.
"""
from __future__ import annotations

import warnings
from dataclasses import dataclass

import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import AlwaysTrue, And, BooleanExpression, EqualTo, In, Or
from pyiceberg.table import Table

from .compact import TablePlan
from .connect_schema import arrow_schema

OFFSET_PREFIX = "cdclab.offset."   # cdclab.offset.<topic>.<partition> = last applied offset
POS_PREFIX = "cdclab.pos."         # cdclab.pos.<topic>.<partition> = "<lastCommitLsn>,<lsn>" high-water position
IN_LIMIT = 200                      # pyiceberg skips file pruning for In() with more literals than this
TABLE_PROPERTIES = {
    "commit.retry.num-retries": "0",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "50",
}


@dataclass
class ApplyResult:
    table: str
    upserted: int
    deleted_keys: int
    truncated: bool
    schema_changed: bool
    backfilled: int = 0


def key_filter(key_names: tuple[str, ...], keys: list[tuple]) -> BooleanExpression:
    """Filter matching any of `keys`, chunked so each In() stays within pyiceberg's pruning limit."""
    if len(key_names) == 1:
        values = [k[0] for k in keys]
        parts = [In(key_names[0], values[i:i + IN_LIMIT]) for i in range(0, len(values), IN_LIMIT)]
    else:
        parts = [And(*[EqualTo(n, v) for n, v in zip(key_names, k, strict=True)]) for k in keys]
    return parts[0] if len(parts) == 1 else Or(*parts)


def _family(t: pa.DataType):
    """Arrow type as Iceberg sees it: small ints are int, large_string is string, and so on."""
    if pa.types.is_integer(t):
        return ("int", 32 if t.bit_width <= 32 else 64)
    if pa.types.is_floating(t):
        return ("float", t.bit_width)
    if pa.types.is_decimal(t):
        return ("decimal", t.precision, t.scale)
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return ("string",)
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return ("binary",)
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return ("list", t.value_type)
    if pa.types.is_timestamp(t):
        return ("timestamp", t.unit, t.tz is not None)
    return ("other", str(t))


def fits(source: pa.DataType, lake: pa.DataType) -> bool:
    """Can every value of `source` be stored in a `lake` column unchanged (same type, or a wider one)?"""
    s, k = _family(source), _family(lake)
    if s[0] != k[0]:
        return False
    if s[0] in ("int", "float"):
        return s[1] <= k[1]
    if s[0] == "decimal":
        return s[2] == k[2] and s[1] <= k[1]
    if s[0] == "list":
        return fits(s[1], k[1])
    return s == k


def promotable(source: pa.DataType, lake: pa.DataType) -> bool:
    """Can Iceberg widen the `lake` column in place so that it fits `source` (int -> long, float -> double,
    decimal precision with the same scale)? A decimal SCALE change is not: union_by_name silently ignores it."""
    s, k = _family(source), _family(lake)
    if s[0] != k[0]:
        return False
    if s[0] in ("int", "float"):
        return True
    if s[0] == "decimal":
        return s[2] == k[2]
    if s[0] == "list":
        return fits(s[1], k[1]) or promotable(s[1], k[1])
    return s == k


class IcebergSink:
    def __init__(self, catalog, namespace: str = "cdc"):
        self.catalog = catalog
        self.namespace = namespace
        try:
            catalog.create_namespace(namespace)
        except Exception:  # noqa: BLE001 - already exists
            pass

    def identifier(self, source_table: str) -> str:
        return f"{self.namespace}.{source_table.replace('.', '__')}"

    def load(self, source_table: str):
        try:
            return self.catalog.load_table(self.identifier(source_table))
        except NoSuchTableError:
            return None

    def stored_offsets(self, source_table: str) -> dict[tuple[str, int], int]:
        """{(topic, partition): last applied offset} recorded in the table's properties."""
        t = self.load(source_table)
        if t is None:
            return {}
        out = {}
        for k, v in t.properties.items():
            if k.startswith(OFFSET_PREFIX):
                topic, _, part = k[len(OFFSET_PREFIX):].rpartition(".")
                out[(topic, int(part))] = int(v)
        return out

    # ------------------------------------------------------------------ schema

    def needs_evolution(self, table: Table, plan: TablePlan) -> bool:
        """True if the plan has columns the table lacks, or a column whose values the table's type cannot hold."""
        current = {f.name: f.type for f in table.schema().as_arrow()}
        return any(c.name not in current or not fits(c.arrow, current[c.name]) for c in plan.columns)

    def evolve(self, table: Table, plan: TablePlan) -> int:
        """Add the plan's new columns and widen changed ones, in their own commit, backfilling database defaults.
        Returns rows backfilled. Raises for a type change Iceberg cannot apply in place."""
        current = {f.name: f.type for f in table.schema().as_arrow()}
        bad = [(c.name, current[c.name], c.arrow) for c in plan.columns
               if c.name in current and not fits(c.arrow, current[c.name]) and not promotable(c.arrow, current[c.name])]
        if bad:
            detail = "; ".join(f"{n}: lake {a} -> source {b}" for n, a, b in bad)
            raise RuntimeError(f"{plan.table}: the source changed column types in a way Iceberg cannot apply in place "
                               f"({detail}). Iceberg only widens int -> long, float -> double and decimal precision "
                               "(same scale). Migrate the lake table yourself (e.g. add a new column, or re-snapshot "
                               "into a new table), then restart the consumer.")
        schema = arrow_schema(plan.columns)
        new_cols = [c for c in plan.columns if c.name not in {f.name for f in table.schema().fields}]
        defaults = {c.name: c.default for c in new_cols if c.default is not None}
        existing = table.scan().to_arrow().to_pylist() if defaults else []
        with table.transaction() as tx:
            with tx.update_schema() as us:   # the context manager is what stages the change in the transaction
                us.union_by_name(schema)
            after = {f.name: f.type for f in tx.table_metadata.schema().as_arrow()}
            still = [c.name for c in plan.columns if not fits(c.arrow, after[c.name])]
            if still:   # raising inside the transaction aborts it: nothing is committed
                raise RuntimeError(f"{plan.table}: schema evolution did not widen {still}; refusing to write")
            if existing:
                # Postgres gave every existing row the default, but Debezium emits no events for them.
                widened = tx.table_metadata.schema().as_arrow()
                rows = [{**r, **defaults} for r in existing]
                tx.delete(AlwaysTrue())
                tx.append(pa.Table.from_pylist([{f.name: r.get(f.name) for f in widened} for r in rows], schema=widened))
        return len(existing)

    def _create(self, plan: TablePlan) -> Table:
        props = {**TABLE_PROPERTIES, "cdclab.source": plan.table, "cdclab.key": ",".join(plan.key_names)}
        return self.catalog.create_table(self.identifier(plan.table), schema=arrow_schema(plan.columns), properties=props)

    # ------------------------------------------------------------------ data

    def _fill_unavailable(self, table, plan: TablePlan) -> None:
        """Copy unchanged-TOAST columns from the stored rows (Postgres did not send them)."""
        if not plan.fill_from_table:
            return
        sources = {k: plan.fill_source.get(k, k) for k in plan.fill_from_table}   # PK changes read the old key
        cols = sorted(set().union(*plan.fill_from_table.values()) | set(plan.key_names))
        lookup = sorted(set(sources.values()))
        stored = table.scan(row_filter=key_filter(plan.key_names, lookup), selected_fields=tuple(cols)).to_arrow().to_pylist()
        by_key = {tuple(r[n] for n in plan.key_names): r for r in stored}
        for k, missing in plan.fill_from_table.items():
            prior = by_key.get(sources[k])
            if prior is None:
                raise RuntimeError(f"{plan.table} key {k}: Postgres omitted unchanged TOAST columns {sorted(missing)}, "
                                   "but the row is not in the lake. Set REPLICA IDENTITY FULL on the source table or "
                                   "re-snapshot it.")
            for c in missing:
                plan.upserts[k][c] = prior[c]

    def apply(self, plan: TablePlan, offsets: dict[tuple[str, int], int], extra_props: dict[str, str] | None = None,
              table: Table | None = None) -> ApplyResult:
        """One fenced commit on `table` (the snapshot the caller read its offsets from), creating it if absent."""
        table = table if table is not None else self._create(plan)
        if self.needs_evolution(table, plan):
            raise RuntimeError(f"{plan.table}: schema evolution must be committed before data (call evolve first)")
        self._fill_unavailable(table, plan)
        props = {f"{OFFSET_PREFIX}{topic}.{part}": str(off) for (topic, part), off in offsets.items()}
        # A row whose last event predates an ADD COLUMN ... DEFAULT has that default in Postgres (but not in its event).
        defaults = {c.name: c.default for c in plan.columns if c.default is not None}
        for k, r in plan.upserts.items():
            known = plan.row_fields.get(k)
            for col, value in defaults.items():
                if known is not None and col not in known:
                    r[col] = value
        rows = list(plan.upserts.values())
        with table.transaction() as tx:
            schema = tx.table_metadata.schema().as_arrow()   # exact names, order, nullability and types
            if plan.truncate:
                tx.delete(AlwaysTrue())
            else:
                touched = sorted(plan.deletes | set(plan.upserts))
                if touched:
                    with warnings.catch_warnings():  # new keys match nothing: expected, not worth a warning
                        warnings.filterwarnings("ignore", message="Delete operation did not match any records")
                        tx.delete(key_filter(plan.key_names, touched))
            # Always append, even nothing: an append creates a snapshot, and only snapshot-producing commits are
            # fenced against concurrent writers. A properties-only commit could silently roll back stored offsets.
            tx.append(pa.Table.from_pylist([{f.name: r.get(f.name) for f in schema} for r in rows], schema=schema))
            tx.set_properties({**props, **(extra_props or {})})
        return ApplyResult(plan.table, len(rows), len(plan.deletes), plan.truncate, False, 0)

    def record_state(self, source_table: str, offsets: dict[tuple[str, int], int], extra_props: dict[str, str] | None = None,
                     table: Table | None = None):
        """Advance stored positions without data changes (e.g. a batch of tombstones or replayed duplicates)."""
        t = table if table is not None else self.load(source_table)
        with t.transaction() as tx:
            tx.append(t.schema().as_arrow().empty_table())   # fence (see apply)
            tx.set_properties({**{f"{OFFSET_PREFIX}{tp}.{p}": str(o) for (tp, p), o in offsets.items()}, **(extra_props or {})})

    def properties(self, source_table: str) -> dict[str, str]:
        t = self.load(source_table)
        return dict(t.properties) if t is not None else {}
