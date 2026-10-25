"""Source schema and a workload that exercises every hard case for CDC into a lakehouse.

    interleaved transactions  T1 writes first but commits after T2 (WAL position order != commit order)
    primary-key change        UPDATE ... SET id = ... (Debezium: delete old key + create new key)
    TOAST                     a large `notes` value, then updates that do not touch it (Postgres omits it)
    schema change             ALTER TABLE ... ADD COLUMN mid-stream
    deletes                   plain deletes, and delete + re-insert of the same key
    edge types                (edge_types) +/-infinity timestamps and dates, arrays incl. a TOASTed one, jsonb, uuid,
                              real, and ALTER COLUMN ... TYPE widenings (int -> bigint, numeric(8,2) -> (12,2))
"""
from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from decimal import Decimal

import psycopg

DDL = """
-- Postgres accepts NaN in any NUMERIC column, but Debezium's precise decimal mode cannot represent it: the task
-- logs "Got a special value (NaN/Infinity) for Decimal type ... set decimal handling mode to 'string'", emits
-- null for a NOT NULL field and FAILS, and restarting replays the same WAL record. Keep NaN out at the source.
CREATE TABLE IF NOT EXISTS public.accounts (
    id          integer PRIMARY KEY,
    name        text NOT NULL,
    balance     numeric(12, 2) NOT NULL CHECK (balance <> 'NaN'::numeric),
    updated_at  timestamp NOT NULL DEFAULT now(),
    notes       text
);
CREATE TABLE IF NOT EXISTS public.orders (
    order_id    bigint PRIMARY KEY,
    account_id  integer NOT NULL,
    amount      numeric(12, 2) NOT NULL CHECK (amount <> 'NaN'::numeric),
    status      text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS public.edge_types (
    id          integer PRIMARY KEY,
    counter     integer,                      -- widened to bigint mid-stream
    amount      numeric(8, 2) CHECK (amount <> 'NaN'::numeric),   -- widened to numeric(12, 2) mid-stream
    ratio       real,
    valid_from  date,                         -- may be 'infinity' / '-infinity'
    valid_to    timestamp,                    -- 'infinity' marks the open-ended row (SCD2 style)
    closed_at   timestamp(3),                 -- millisecond precision: a different Debezium type and sentinel
    seen_at     timestamptz,                  -- may be '-infinity' / 'infinity'
    tags        text[],
    amounts     numeric(8, 2)[],
    doc         jsonb,
    ref         uuid,
    blob        text[]                        -- sometimes large (TOASTed): updates that skip it send a placeholder
);
"""


@dataclass
class Stats:
    inserts: int = 0
    updates: int = 0
    deletes: int = 0
    pk_changes: int = 0
    toast_updates: int = 0
    interleaved: int = 0
    schema_changes: int = 0
    edge_changes: int = 0


def setup(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute(DDL)


def _big_text(rng: random.Random) -> str:
    # > 2 kB of incompressible text so Postgres stores it out of line (TOAST).
    return "".join(rng.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(9000))


def _edge_values(rng: random.Random, big_counter: bool) -> tuple:
    inf = rng.random() < 0.4
    return (
        rng.randrange(2**31, 2**40) if big_counter and rng.random() < 0.5 else rng.randrange(-2**31, 2**31),
        round(rng.uniform(-99_999, 99_999), 2),
        rng.choice([0.1, 1.5, -2.75, 3.4028234e38, rng.uniform(-1e6, 1e6)]),
        rng.choice(["infinity", "-infinity"]) if inf else f"20{rng.randrange(10, 30)}-0{rng.randrange(1, 10)}-1{rng.randrange(0, 10)}",
        "infinity" if inf else f"2024-0{rng.randrange(1, 10)}-11 12:34:56.{rng.randrange(0, 999999):06d}",
        rng.choice(["infinity", "-infinity", "2024-03-04 05:06:07.891"]),
        rng.choice(["infinity", "-infinity", "2024-03-04 05:06:07.123456+05:30"]),
        rng.choice([[], ["a", None, "b,c"], [f"t{rng.randrange(100)}"]]),
        rng.choice([None, [], [Decimal("1.50"), None, Decimal("-0.01")]]),
        rng.choice([None, '{"b": 2, "a": [1, {"x": null}]}', '[1, "two", 3.5]', '"just a string"']),
        str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        [_big_text(rng)[:60] for _ in range(150)] if rng.random() < 0.3 else ["small"],
    )


def _edge_step(c, rng: random.Random, ids: list[int], next_id: int, big_counter: bool) -> int:
    r = rng.random()
    if r < 0.5 or len(ids) < 3:
        c.execute("INSERT INTO public.edge_types (id, counter, amount, ratio, valid_from, valid_to, closed_at, seen_at, "
                  "tags, amounts, doc, ref, blob) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                  (next_id, *_edge_values(rng, big_counter)))
        ids.append(next_id)
        return next_id + 1
    if r < 0.85:   # does not touch blob: a large blob arrives as the array form of the TOAST placeholder
        v = _edge_values(rng, big_counter)
        c.execute("UPDATE public.edge_types SET counter = %s, ratio = %s, valid_to = %s, tags = %s WHERE id = %s",
                  (v[0], v[2], v[4], v[7], rng.choice(ids)))
    else:
        c.execute("DELETE FROM public.edge_types WHERE id = %s", (ids.pop(rng.randrange(len(ids))),))
    return next_id


def run(dsn: str, steps: int = 300, seed: int = 7, schema_change_at: int | None = 150) -> Stats:
    rng = random.Random(seed)
    erng = random.Random(seed * 7919 + 1)   # separate stream: the accounts/orders sequence stays as before
    st = Stats()
    with psycopg.connect(dsn, autocommit=True) as c, psycopg.connect(dsn) as c2:
        ids = [r[0] for r in c.execute("SELECT id FROM public.accounts").fetchall()]
        next_id = max(ids, default=0) + 1
        next_order = (c.execute("SELECT coalesce(max(order_id), 0) FROM public.orders").fetchone()[0] or 0) + 1
        has_tier = bool(c.execute("SELECT 1 FROM information_schema.columns WHERE table_name = 'accounts' "
                                  "AND column_name = 'tier'").fetchone())
        edge_ids = [r[0] for r in c.execute("SELECT id FROM public.edge_types").fetchall()]
        next_edge = max(edge_ids, default=0) + 1
        widened = c.execute("SELECT data_type FROM information_schema.columns WHERE table_name = 'edge_types' "
                            "AND column_name = 'counter'").fetchone()[0] == "bigint"
        for step in range(steps):
            if schema_change_at is not None and step == schema_change_at and not has_tier:
                c.execute("ALTER TABLE public.accounts ADD COLUMN tier text DEFAULT 'standard'")
                has_tier, st.schema_changes = True, st.schema_changes + 1
            if schema_change_at is not None and step == schema_change_at and not widened:
                c.execute("ALTER TABLE public.edge_types ALTER COLUMN counter TYPE bigint, "
                          "ALTER COLUMN amount TYPE numeric(12, 2)")
                widened, st.schema_changes = True, st.schema_changes + 1
            if erng.random() < 0.15:
                next_edge = _edge_step(c, erng, edge_ids, next_edge, widened)
                st.edge_changes += 1
            r = rng.random()
            if r < 0.30 or len(ids) < 5:
                notes = _big_text(rng) if rng.random() < 0.2 else f"note {next_id}"
                c.execute("INSERT INTO public.accounts (id, name, balance, notes) VALUES (%s, %s, %s, %s)",
                          (next_id, f"user{next_id}", round(rng.uniform(0, 10_000), 2), notes))
                ids.append(next_id)
                next_id += 1
                st.inserts += 1
            elif r < 0.55:
                # Touches balance only: a large `notes` value is not re-sent (TOAST placeholder downstream).
                c.execute("UPDATE public.accounts SET balance = balance + %s, updated_at = now() WHERE id = %s",
                          (round(rng.uniform(-50, 50), 2), rng.choice(ids)))
                st.updates += 1
            elif r < 0.62:
                c.execute("UPDATE public.accounts SET notes = %s WHERE id = %s", (_big_text(rng), rng.choice(ids)))
                st.toast_updates += 1
            elif r < 0.72:
                victim = ids.pop(rng.randrange(len(ids)))
                c.execute("DELETE FROM public.orders WHERE account_id = %s", (victim,))
                c.execute("DELETE FROM public.accounts WHERE id = %s", (victim,))
                st.deletes += 1
            elif r < 0.76:
                old = ids.pop(rng.randrange(len(ids)))
                c.execute("UPDATE public.orders SET account_id = %s WHERE account_id = %s", (next_id, old))
                c.execute("UPDATE public.accounts SET id = %s WHERE id = %s", (next_id, old))
                ids.append(next_id)
                next_id += 1
                st.pk_changes += 1
            elif r < 0.88:
                c.execute("INSERT INTO public.orders (order_id, account_id, amount, status) VALUES (%s, %s, %s, %s)",
                          (next_order, rng.choice(ids), round(rng.uniform(1, 500), 2), rng.choice(["new", "paid"])))
                next_order += 1
                st.inserts += 1
            else:
                # T1 writes first (lower WAL position) but commits after T2.
                a, b = rng.sample(ids, 2)
                with c2.transaction():
                    c2.execute("UPDATE public.accounts SET balance = balance + 1, updated_at = now() WHERE id = %s", (a,))
                    c.execute("UPDATE public.accounts SET balance = balance - 1, updated_at = now() WHERE id = %s", (b,))
                st.interleaved += 1
    return st
