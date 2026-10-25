"""Regression tests for defects found by adversarial review. Each test failed against the code it was written for."""
import datetime as dt

import pytest
from debezium_fixtures import ROW_FIELDS, record, row, truncate
from pyiceberg.catalog.sql import SqlCatalog

from cdclab.connect_schema import column
from cdclab.consumer import Applier, applied_offsets, awaiting_create_half, split_trailing_transaction
from cdclab.events import TOAST_PLACEHOLDER, is_placeholder, parse, position
from cdclab.reconcile import compare
from cdclab.sink import IcebergSink

TOMBSTONE = (b'{"id": 1}', None, "lab.public.accounts", 0, 99)


@pytest.fixture
def sink(tmp_path):
    wh = tmp_path.as_posix()
    return IcebergSink(SqlCatalog("lab", uri=f"sqlite:///{wh}/cat.db", warehouse=wh))


def rec(op, offset, **kw):
    return record(op, offset=offset, **kw)


def lake(sink, table="public.accounts"):
    t = sink.load(table)
    return {r["id"]: r for r in t.scan().to_arrow().to_pylist()} if t else {}


def test_truncate_only_batch_is_applied(sink):
    a = Applier(sink, "lab")
    a.apply([rec("c", 0, after=row(1)), rec("c", 1, after=row(2))])
    k, v, t, p, _ = truncate(offset=2)
    a.apply([(k, v, t, p, 2)])                       # a batch holding nothing but the truncate
    assert lake(sink) == {}
    assert sink.stored_offsets("public.accounts")[("lab.public.accounts", 0)] == 2


def test_truncate_has_a_position_so_its_replay_is_dropped(sink):
    k, v, *_ = truncate(offset=1)
    a = Applier(sink, "lab")
    a.apply([rec("c", 0, after=row(1), seq=(10, 100))])
    import json
    tv = json.loads(v)
    tv["payload"]["source"]["sequence"] = json.dumps(["20", "200"])
    tv = json.dumps(tv).encode()
    assert parse(k, tv, "lab.public.accounts", 0, 1).position == (20, 200)
    a.apply([(None, tv, "lab.public.accounts", 0, 1), rec("c", 2, after=row(2), seq=(30, 300))])
    assert set(lake(sink)) == {2}
    a.apply([(None, tv, "lab.public.accounts", 0, 3)])   # Kafka Connect replays the truncate at a new offset
    assert set(lake(sink)) == {2}, "a replayed truncate must not wipe rows inserted after it"


def test_replay_of_last_pk_change_pair_is_dropped_not_crash(sink):
    """The last applied events were d(1)+c(7) at one position. A Connect replay re-sends both; the create half
    carries a TOAST placeholder whose only source (the old key's row) is gone. Both must be skipped."""
    a = Applier(sink, "lab")
    a.apply([rec("c", 0, after=row(1, notes="big" * 2000), seq=(10, 100))])
    pair = [rec("d", 1, before=row(1), seq=(20, 200)),
            rec("c", 2, after=row(7, notes=TOAST_PLACEHOLDER), seq=(20, 200), old_key={"id": 1})]
    a.apply(pair)
    assert lake(sink)[7]["notes"] == "big" * 2000
    replay = [rec("d", 3, before=row(1), seq=(20, 200)),
              rec("c", 4, after=row(7, notes=TOAST_PLACEHOLDER), seq=(20, 200), old_key={"id": 1})]
    a.apply(replay)
    assert set(lake(sink)) == {7} and a.skipped_replays == 2


def test_stale_writer_cannot_reapply_a_batch(sink):
    """Two appliers share a table. Writer B applied the batch; writer A's in-memory view is irrelevant: it reads the
    offsets from the snapshot it commits against, so nothing is applied twice and no offset regresses."""
    batch = [rec("c", 0, after=row(1, notes="v1" * 3000), seq=(10, 100)),
             rec("u", 1, after=row(1, balance="5.00", notes=TOAST_PLACEHOLDER), seq=(20, 200))]
    a, b = Applier(sink, "lab"), Applier(IcebergSink(sink.catalog), "lab")
    a.apply(batch[:1])
    b.apply(batch[1:])                                     # B wins: balance 5.00, notes kept
    later = [rec("u", 2, after=row(1, balance="9.00", notes="v2" * 3000), seq=(30, 300))]
    b.apply(later)
    assert a.apply(batch[1:]) == []                        # A re-sees record 1: already applied, nothing happens
    r = lake(sink)[1]
    assert str(r["balance"]) == "9.00" and r["notes"] == "v2" * 3000
    assert sink.stored_offsets("public.accounts")[("lab.public.accounts", 0)] == 2


def test_interleaved_transactions_use_the_sequence_pair_not_raw_lsn(sink):
    """T2 commits first with a higher lsn; T1 commits later with a LOWER lsn. The (lastCommitLsn, lsn) pair still
    increases, so T1 is applied. Swapping the pair's order would wrongly treat T1 as a replay."""
    assert position({"sequence": '["100", "2000"]'}, "c") < position({"sequence": '["2050", "1500"]'}, "c")
    a = Applier(sink, "lab")
    a.apply([rec("c", 0, after=row(1), seq=(100, 2000))])          # T2
    a.apply([rec("c", 1, after=row(2), seq=(2050, 1500))])         # T1: lsn 1500 < 2000, but committed later
    assert set(lake(sink)) == {1, 2} and a.skipped_replays == 0
    a.apply([rec("c", 2, after=row(1), seq=(100, 2000))])          # genuine replay of T2
    assert a.skipped_replays == 1


def test_bytea_and_array_placeholders_are_recognised():
    assert is_placeholder("X19kZWJleml1bV91bmF2YWlsYWJsZV92YWx1ZQ==")       # bytea, as base64 in JSON
    assert is_placeholder(b"__debezium_unavailable_value")
    assert is_placeholder(["__debezium_unavailable_value"])              # text[]
    assert not is_placeholder("X19kZWJleml1bV91bmF2YWlsYWJsZV92YWx1ZQ==x") and not is_placeholder([]) and not is_placeholder(42)
    fields = ROW_FIELDS + [{"type": "bytes", "optional": True, "field": "blob"}]
    e = parse(*record("u", after={**row(1), "blob": "X19kZWJleml1bV91bmF2YWlsYWJsZV92YWx1ZQ=="}, row_fields=fields))
    assert e.unavailable == {"blob"}


def test_time_columns_decode_to_times():
    assert column({"field": "t", "type": "int32", "name": "io.debezium.time.Time"}).decode(3_600_001) == dt.time(1, 0, 0, 1000)
    assert column({"field": "t", "type": "int64", "name": "io.debezium.time.NanoTime"}).decode(1_000) == dt.time(0, 0, 0, 1)


def test_reconcile_sees_duplicate_keys():
    src = [{"id": 1, "v": "a"}]
    d = compare("t", ["id"], src, [{"id": 1, "v": "a"}, {"id": 1, "v": "a"}])
    assert not d.match and d.duplicate_in_lake == {(1,): 2} and "duplicate keys" in d.describe()


def test_partial_batch_holds_trailing_transaction_and_pk_delete():
    one_tx = [rec("c", i, after=row(i), tx=9) for i in range(3)]
    assert split_trailing_transaction(one_tx, full=False) == ([], one_tx)      # might continue: hold
    assert split_trailing_transaction(one_tx, full=True) == (one_tx, [])       # filled the poll: apply
    d = rec("d", 5, before=row(1), tx=7)
    d = (*d[:5], [("__debezium.newkey", b'{"id": 2}')])
    assert awaiting_create_half([rec("c", 4, after=row(3), tx=6), d, TOMBSTONE])
    assert not awaiting_create_half([d, rec("c", 6, after=row(2), tx=7, old_key={"id": 1})])


def test_kafka_commit_covers_only_applied_records():
    """Held records are not committed to Kafka; flushing them later commits explicit offsets (no implicit-commit error)."""
    ready = [rec("c", 4, after=row(1), partition=0), rec("c", 9, after=row(2), partition=1), rec("c", 7, after=row(3), partition=0)]
    assert applied_offsets(ready) == {("lab.public.accounts", 0): 8, ("lab.public.accounts", 1): 10}
    assert applied_offsets([]) == {}
