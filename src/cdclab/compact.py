"""Collapse a micro-batch of change events into the final state per key, per table.

Events for one key arrive in order (Debezium keys messages by primary key, so they share a partition). Within a batch:

    create / read / update -> the row as it is after the event
    delete                 -> the key is gone
    delete then create     -> the new row (an upsert)
    create then delete     -> gone (and a no-op if the row never existed in the lake)
    truncate               -> everything before it in this table is discarded; the table is emptied first

Unchanged TOAST columns arrive as a placeholder: Postgres did not send them. Their value is taken from the latest
earlier state of the row in the batch, or, failing that, read from the lake (`fill_from_table`). A primary-key change
arrives as delete(old key) + create(new key), and the create can carry placeholders too: then the value lives under
the OLD key (`fill_source`), which Debezium names in the __debezium.oldkey header.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .events import ChangeEvent


@dataclass
class TablePlan:
    table: str
    truncate: bool = False
    upserts: dict[tuple, dict] = field(default_factory=dict)        # key -> full row
    deletes: set[tuple] = field(default_factory=set)
    fill_from_table: dict[tuple, set[str]] = field(default_factory=dict)  # key -> columns to copy from the stored row
    fill_source: dict[tuple, tuple] = field(default_factory=dict)   # key -> stored key to copy from (PK change)
    row_fields: dict[tuple, frozenset[str]] = field(default_factory=dict)  # key -> columns its last event knew about
    key_names: tuple[str, ...] = ()
    columns: list = field(default_factory=list)                      # latest row schema seen in the batch
    events: int = 0


def compact(events: list[ChangeEvent]) -> dict[str, TablePlan]:
    plans: dict[str, TablePlan] = {}
    gone: dict[str, dict[tuple, dict]] = {}  # table -> rows deleted in this batch (their last state), for PK changes
    for ev in events:
        plan = plans.setdefault(ev.table, TablePlan(ev.table))
        dead = gone.setdefault(ev.table, {})
        plan.events += 1
        if ev.op == "t":
            plans[ev.table] = TablePlan(ev.table, truncate=True, events=plan.events,
                                        key_names=plan.key_names, columns=plan.columns)
            gone[ev.table] = {}
            continue
        plan.key_names = ev.key_names or plan.key_names
        if not plan.key_names:
            raise ValueError(f"{ev.table} has no primary key: Debezium sends a null message key, so changes cannot be "
                             "applied by key. Add a primary key (or set message.key.columns on the connector).")
        if ev.columns:
            plan.columns = ev.columns
        k = ev.key
        if ev.op == "d":
            if k in plan.upserts:
                dead[k] = (plan.upserts.pop(k), plan.fill_from_table.pop(k, set()), plan.fill_source.pop(k, k))
            else:
                dead[k] = (None, set(), k)   # the row's last state is in the lake (if anywhere)
            plan.deletes.add(k)
            continue
        if ev.op not in ("c", "u", "r"):
            raise ValueError(f"unknown op {ev.op!r} in {ev.table}")
        row = dict(ev.after or {})
        missing = ev.unavailable
        # Where the previous state of this row lives: this batch (same key), the old key of a PK change, or the lake.
        if k in plan.upserts:
            prev_row, prev_missing, prev_source = plan.upserts[k], plan.fill_from_table.get(k, set()), plan.fill_source.get(k, k)
        elif ev.old_key is not None and ev.old_key in dead:
            prev_row, prev_missing, prev_source = dead[ev.old_key]
        elif ev.old_key is not None:
            prev_row, prev_missing, prev_source = None, set(), ev.old_key
        elif plan.truncate or k in plan.deletes:
            prev_row, prev_missing, prev_source = None, set(), None   # the row was removed in this batch
        else:
            prev_row, prev_missing, prev_source = None, set(), k
        still_missing = set()
        for col in missing:
            if prev_row is not None and col not in prev_missing:
                row[col] = prev_row[col]              # known from an earlier state in this batch
            elif prev_source is None:
                raise ValueError(f"{ev.table} key {k}: unchanged TOAST column {col!r} after the row was deleted in "
                                 "the same batch; this cannot happen with a consistent source")
            else:
                still_missing.add(col)                # must come from the stored row at prev_source
        for col in still_missing:
            row[col] = None
        plan.deletes.discard(k)
        plan.upserts[k] = row
        plan.row_fields[k] = frozenset(c.name for c in ev.columns)
        if still_missing:
            plan.fill_from_table[k] = still_missing
            plan.fill_source[k] = prev_source
        else:
            plan.fill_from_table.pop(k, None)
            plan.fill_source.pop(k, None)
    return plans
