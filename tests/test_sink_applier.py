"""The exactly-once core, against a real local Iceberg catalog (no Kafka needed)."""
from decimal import Decimal

import pytest
from debezium_fixtures import ROW_FIELDS, record, row, truncate
from pyiceberg.catalog.sql import SqlCatalog

from cdclab.consumer import Applier
from cdclab.events import TOAST_PLACEHOLDER
from cdclab.sink import IcebergSink


@pytest.fixture
def sink(tmp_path):
    wh = tmp_path.as_posix()
    return IcebergSink(SqlCatalog("lab", uri=f"sqlite:///{wh}/cat.db", warehouse=wh))


def rec(op, offset, **kw):
    return record(op, offset=offset, **kw)


def lake(sink, table="public.accounts"):
    t = sink.load(table)
    return {r["id"]: r for r in t.scan().to_arrow().to_pylist()} if t else {}


def test_batches_apply_and_offsets_commit_with_data(sink):
    a = Applier(sink, "lab")
    a.apply([rec("c", 0, after=row(1, "a")), rec("c", 1, after=row(2, "b", "5.50")), rec("u", 2, after=row(1, "a2"))])
    assert {k: v["name"] for k, v in lake(sink).items()} == {1: "a2", 2: "b"}
    assert lake(sink)[2]["balance"] == Decimal("5.50")
    assert sink.stored_offsets("public.accounts") == {("lab.public.accounts", 0): 2}
    a.apply([rec("d", 3, before=row(2)), (b'{"id": 2}', None, "lab.public.accounts", 0, 4)])  # delete + tombstone
    assert set(lake(sink)) == {1} and sink.stored_offsets("public.accounts")[("lab.public.accounts", 0)] == 4


def test_replay_after_crash_is_idempotent(sink):
    batch = [rec("c", 0, after=row(1)), rec("u", 1, after=row(1, balance="20.00")), rec("c", 2, after=row(2))]
    Applier(sink, "lab").apply(batch)
    before = sink.load("public.accounts").current_snapshot().snapshot_id
    # A restarted consumer re-reads the same records (Kafka offsets were never committed): nothing changes.
    Applier(sink, "lab").apply(batch)
    assert sink.load("public.accounts").current_snapshot().snapshot_id == before
    assert lake(sink)[1]["balance"] == Decimal("20.00")


def test_failed_transaction_leaves_no_partial_state(sink, monkeypatch):
    a = Applier(sink, "lab")
    a.apply([rec("c", 0, after=row(1))])
    from pyiceberg.table import Transaction

    def boom(self, *args, **kw):
        raise RuntimeError("crash mid-commit")
    monkeypatch.setattr(Transaction, "append", boom)
    with pytest.raises(RuntimeError):
        a.apply([rec("d", 1, before=row(1)), rec("c", 2, after=row(5))])
    monkeypatch.undo()
    assert set(lake(sink)) == {1}  # the delete staged before the crash was not committed
    assert sink.stored_offsets("public.accounts")[("lab.public.accounts", 0)] == 0
    Applier(sink, "lab").apply([rec("d", 1, before=row(1)), rec("c", 2, after=row(5))])
    assert set(lake(sink)) == {5}


def test_out_of_order_lsn_is_not_skipped(sink):
    # Interleaved transactions: T2 (lsn 2000) commits first and is emitted first; T1 (lsn 1500) commits later.
    # Its raw lsn is below the stored high-water mark, but its (lastCommitLsn, lsn) position is above it, so it is
    # applied. Separate batches, so the second really is compared with a stored position.
    a = Applier(sink, "lab")
    a.apply([rec("c", 0, after=row(1), lsn=2000, seq=(100, 2000))])
    a.apply([rec("c", 1, after=row(2), lsn=1500, seq=(2050, 1500))])
    assert set(lake(sink)) == {1, 2} and a.skipped_replays == 0


def test_truncate_then_insert(sink):
    a = Applier(sink, "lab")
    a.apply([rec("c", 0, after=row(1)), rec("c", 1, after=row(2))])
    k, v, t, p, _ = truncate(offset=2)
    a.apply([(k, v, t, p, 2), rec("c", 3, after=row(3))])
    assert set(lake(sink)) == {3}


def test_toast_column_kept_from_lake(sink):
    a = Applier(sink, "lab")
    a.apply([rec("c", 0, after=row(1, notes="x" * 5000))])
    a.apply([rec("u", 1, after=row(1, balance="99.00", notes=TOAST_PLACEHOLDER))])
    r = lake(sink)[1]
    assert r["balance"] == Decimal("99.00") and r["notes"] == "x" * 5000


def test_schema_evolution_adds_column(sink):
    a = Applier(sink, "lab")
    a.apply([rec("c", 0, after=row(1))])
    fields = ROW_FIELDS + [{"type": "string", "optional": True, "field": "tier"}]
    a.apply([rec("c", 1, after={**row(2), "tier": "gold"}, row_fields=fields)])
    rows = lake(sink)
    assert rows[2]["tier"] == "gold" and rows[1]["tier"] is None


def test_multiple_partitions_tracked_separately(sink):
    a = Applier(sink, "lab")
    a.apply([rec("c", 0, after=row(1), partition=0), rec("c", 0, after=row(2), partition=1)])
    a.apply([rec("d", 1, before=row(1), partition=0)])
    assert set(lake(sink)) == {2}
    assert sink.stored_offsets("public.accounts") == {("lab.public.accounts", 0): 1, ("lab.public.accounts", 1): 0}


def test_update_existing_key_after_schema_change(sink):
    """pyiceberg#3105: upsert fails for matched keys after a column is added. delete + append does not."""
    a = Applier(sink, "lab")
    a.apply([rec("c", 0, after=row(1)), rec("c", 1, after=row(2))])
    fields = ROW_FIELDS + [{"type": "string", "optional": True, "field": "tier"}]
    a.apply([rec("u", 2, after={**row(1, "renamed"), "tier": "gold"}, row_fields=fields)])
    rows = lake(sink)
    assert rows[1]["name"] == "renamed" and rows[1]["tier"] == "gold" and rows[2]["tier"] is None


def test_added_column_default_is_backfilled(sink):
    """ALTER TABLE ADD COLUMN ... DEFAULT x gives every existing source row x, but Debezium emits no events for them."""
    a = Applier(sink, "lab")
    a.apply([rec("c", 0, after=row(1)), rec("c", 1, after=row(2))])
    fields = ROW_FIELDS + [{"type": "string", "optional": True, "default": "standard", "field": "tier"}]
    res = a.apply([rec("c", 2, after={**row(3), "tier": "gold"}, row_fields=fields)])
    assert res[0].schema_changed and res[0].backfilled == 2
    assert {k: v["tier"] for k, v in lake(sink).items()} == {1: "standard", 2: "standard", 3: "gold"}


def test_many_keys_delete_is_chunked(sink):
    from cdclab.sink import key_filter
    f = key_filter(("id",), [(i,) for i in range(450)])
    assert type(f).__name__ == "Or"  # 3 In() chunks of <= 200 values: pyiceberg keeps pruning files
    a = Applier(sink, "lab")
    a.apply([rec("c", i, after=row(i)) for i in range(450)])
    a.apply([rec("d", 450 + i, before=row(i)) for i in range(300)])
    assert len(lake(sink)) == 150


def test_commit_conflict_rereads_offsets_and_skips(sink, monkeypatch):
    """Another writer commits the same batch first: the retry finds it applied and does nothing."""
    from pyiceberg.exceptions import CommitFailedException

    batch = [rec("c", 0, after=row(1)), rec("u", 1, after=row(1, "b"))]
    a = Applier(sink, "lab")
    real_apply = sink.apply
    calls = {"n": 0}

    def racing_apply(plan, offsets, *extra, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            Applier(IcebergSink(sink.catalog), "lab").apply(batch)   # the other writer wins the race
            raise CommitFailedException("Requirement failed: branch main has changed")
        return real_apply(plan, offsets, *extra, **kw)
    monkeypatch.setattr(sink, "apply", racing_apply)
    assert a.apply(batch) == []          # retry re-read offsets: already applied, nothing more to do
    assert calls["n"] == 1 and lake(sink)[1]["name"] == "b"


def test_tables_disable_builtin_commit_retry(sink):
    Applier(sink, "lab").apply([rec("c", 0, after=row(1))])
    assert sink.load("public.accounts").properties["commit.retry.num-retries"] == "0"


def test_pk_change_keeps_toast_value_from_old_key_in_lake(sink):
    """UPDATE accounts SET id = 117 WHERE id = 42 with a large untouched `notes`: Debezium emits d(42) + c(117),
    and the create carries the TOAST placeholder. The value must come from the OLD key's stored row."""
    a = Applier(sink, "lab")
    a.apply([rec("c", 0, after=row(42, notes="y" * 9000))])
    a.apply([rec("d", 1, before=row(42), tx=7), (b'{"id": 42}', None, "lab.public.accounts", 0, 2),
             rec("c", 3, after=row(117, notes=TOAST_PLACEHOLDER), tx=7, old_key={"id": 42})])
    rows = lake(sink)
    assert set(rows) == {117} and rows[117]["notes"] == "y" * 9000


def test_pk_change_keeps_toast_value_from_old_key_in_batch(sink):
    a = Applier(sink, "lab")
    a.apply([rec("c", 0, after=row(42, notes="z" * 9000)), rec("d", 1, before=row(42), tx=7),
             rec("c", 2, after=row(117, notes=TOAST_PLACEHOLDER), tx=7, old_key={"id": 42})])
    assert lake(sink)[117]["notes"] == "z" * 9000


def test_split_trailing_transaction():
    from cdclab.consumer import split_trailing_transaction
    recs = [rec("c", 0, after=row(1), tx=1), rec("d", 1, before=row(2), tx=2),
            (b'{"id": 2}', None, "lab.public.accounts", 0, 2), rec("c", 3, after=row(3), tx=2)]
    ready, held = split_trailing_transaction(recs)
    assert [r[4] for r in ready] == [0] and [r[4] for r in held] == [1, 2, 3]
    one_tx = [rec("c", i, after=row(i), tx=9) for i in range(3)]
    assert split_trailing_transaction(one_tx, full=True) == (one_tx, [])   # filled the poll: apply, do not stall
    assert split_trailing_transaction(one_tx, full=False) == ([], one_tx)  # may continue: hold until idle or more


def test_rows_older_than_added_column_get_its_default_in_same_batch(sink):
    """Rows whose last event predates ADD COLUMN ... DEFAULT get the default, as Postgres gave it to them."""
    fields = ROW_FIELDS + [{"type": "string", "optional": True, "default": "standard", "field": "tier"}]
    Applier(sink, "lab").apply([rec("c", 0, after=row(1)), rec("c", 1, after=row(2)),
                                rec("u", 2, after={**row(2, "b"), "tier": "gold"}, row_fields=fields),
                                rec("c", 3, after={**row(3), "tier": None}, row_fields=fields)])
    assert {k: v["tier"] for k, v in lake(sink).items()} == {1: "standard", 2: "gold", 3: None}  # explicit NULL stays NULL


def test_kafka_connect_replay_is_filtered_by_debezium_position(sink):
    """Debezium is at-least-once: after a Connect crash it re-emits events at NEW offsets. With TOAST merging,
    re-applying an older update after a newer one could leave a stale value; the position filter drops the replay."""
    a = Applier(sink, "lab")
    a.apply([rec("c", 0, after=row(1, notes="v1" * 3000), seq=(10, 100)),
             rec("u", 1, after=row(1, notes="v2" * 3000), seq=(20, 200)),
             rec("u", 2, after=row(1, balance="7.00", notes=TOAST_PLACEHOLDER), seq=(30, 300))])
    # Connect crashed and replays the middle of the stream at offsets 3..4 (same positions as before).
    res = a.apply([rec("u", 3, after=row(1, notes="v1" * 3000), seq=(10, 100)),
                   rec("u", 4, after=row(1, notes="v2" * 3000), seq=(20, 200))])
    r = lake(sink)[1]
    assert r["notes"] == "v2" * 3000 and r["balance"] == Decimal("7.00")
    assert a.skipped_replays == 2 and res[0].upserted == 0                   # nothing re-applied ...
    assert sink.stored_offsets("public.accounts")[("lab.public.accounts", 0)] == 4   # ... but offsets advanced
    # A restarted consumer knows the high-water mark from the table, not from memory.
    b = Applier(sink, "lab")
    b.apply([rec("u", 5, after=row(1, notes="v1" * 3000), seq=(10, 100))])
    assert b.skipped_replays == 1 and lake(sink)[1]["notes"] == "v2" * 3000


def test_snapshot_reads_and_ties_bypass_the_position_filter(sink):
    a = Applier(sink, "lab")
    a.apply([rec("u", 0, after=row(1), seq=(50, 500))])
    a.apply([rec("r", 1, after=row(2), seq=(10, 100)),                      # snapshot read: no position
             rec("d", 2, before=row(1), seq=(60, 600)),                     # PK-change halves share one
             rec("c", 3, after=row(3), seq=(60, 600), old_key={"id": 1})])  # position, within the batch
    assert set(lake(sink)) == {2, 3} and a.skipped_replays == 0
