"""Source schema and a workload that exercises every hard case for CDC into a lakehouse.

    interleaved transactions  T1 writes first but commits after T2 (WAL position order != commit order)
    primary-key change        UPDATE ... SET id = ... (Debezium: delete old key + create new key)
    TOAST                     a large `notes` value, then updates that do not touch it (Postgres omits it)
    schema change             ALTER TABLE ... ADD COLUMN mid-stream
    deletes                   plain deletes, and delete + re-insert of the same key
"""
from __future__ import annotations

import random
from dataclasses import dataclass

import psycopg

DDL = """
CREATE TABLE IF NOT EXISTS public.accounts (
    id          integer PRIMARY KEY,
    name        text NOT NULL,
    balance     numeric(12, 2) NOT NULL,
    updated_at  timestamp NOT NULL DEFAULT now(),
    notes       text
);
CREATE TABLE IF NOT EXISTS public.orders (
    order_id    bigint PRIMARY KEY,
    account_id  integer NOT NULL,
    amount      numeric(12, 2) NOT NULL,
    status      text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
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


def setup(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute(DDL)


def _big_text(rng: random.Random) -> str:
    # > 2 kB of incompressible text so Postgres stores it out of line (TOAST).
    return "".join(rng.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(9000))


def run(dsn: str, steps: int = 300, seed: int = 7, schema_change_at: int | None = 150) -> Stats:
    rng = random.Random(seed)
    st = Stats()
    with psycopg.connect(dsn, autocommit=True) as c, psycopg.connect(dsn) as c2:
        ids = [r[0] for r in c.execute("SELECT id FROM public.accounts").fetchall()]
        next_id = max(ids, default=0) + 1
        next_order = (c.execute("SELECT coalesce(max(order_id), 0) FROM public.orders").fetchone()[0] or 0) + 1
        has_tier = bool(c.execute("SELECT 1 FROM information_schema.columns WHERE table_name = 'accounts' "
                                  "AND column_name = 'tier'").fetchone())
        for step in range(steps):
            if schema_change_at is not None and step == schema_change_at and not has_tier:
                c.execute("ALTER TABLE public.accounts ADD COLUMN tier text DEFAULT 'standard'")
                has_tier, st.schema_changes = True, st.schema_changes + 1
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
