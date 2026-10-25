"""Regressions from the second adversarial review (type changes, special values, arrays, batching, fencing).

Each Debezium encoding used here was checked against Debezium 3.7.0.Final on the lab stack, not taken from docs.
"""
import datetime as dt
import json
from decimal import Decimal

import pyarrow as pa
import pytest
from debezium_fixtures import ROW_FIELDS, dec, record, row
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import CommitFailedException

from cdclab.compact import compact
from cdclab.connect_schema import _decimal_decoder, column
from cdclab.consumer import Applier, hold_unpaired_deletes, run
from cdclab.events import TOAST_PLACEHOLDER, parse
from cdclab.reconcile import _float32, compare, normalise
from cdclab.sink import IcebergSink, fits, promotable

T = "lab.public.accounts"


@pytest.fixture
def sink(tmp_path):
    wh = tmp_path.as_posix()
    return IcebergSink(SqlCatalog("lab", uri=f"sqlite:///{wh}/cat.db", warehouse=wh))


def lake(sink, table="public.accounts"):
    t = sink.load(table)
    return {r["id"]: r for r in t.scan().to_arrow().to_pylist()} if t else {}


def fields(**extra):
    """ROW_FIELDS plus extra columns, given as Connect field dicts."""
    return ROW_FIELDS + [{**f, "field": name} for name, f in extra.items()]


INT32 = {"type": "int32", "optional": True}
INT64 = {"type": "int64", "optional": True}


def decimal_field(precision, scale):
    return {"type": "bytes", "optional": True, "name": "org.apache.kafka.connect.data.Decimal", "version": 1,
            "parameters": {"scale": str(scale), "connect.decimal.precision": str(precision)}}


# ---------------------------------------------------------------- type changes

def test_int_widened_to_bigint_is_evolved_and_big_values_land(sink):
    """ALTER COLUMN counter TYPE bigint: the schema still has the name, but values over 2**31 no longer fit."""
    a = Applier(sink, "lab")
    a.apply([record("c", after={**row(1), "counter": 5}, row_fields=fields(counter=INT32), offset=0, seq=(1, 1))])
    res = a.apply([record("c", after={**row(2), "counter": 2**40}, row_fields=fields(counter=INT64), offset=1,
                          seq=(2, 2))])
    assert res[0].schema_changed
    assert {k: v["counter"] for k, v in lake(sink).items()} == {1: 5, 2: 2**40}
    assert str(sink.load("public.accounts").schema().find_field("counter").field_type) == "long"


def test_decimal_precision_and_float_widening_are_evolved(sink):
    a = Applier(sink, "lab")
    small = fields(amount=decimal_field(8, 2), ratio={"type": "float", "optional": True})
    big = fields(amount=decimal_field(12, 2), ratio={"type": "double", "optional": True})
    a.apply([record("c", after={**row(1), "amount": dec("1.50"), "ratio": 0.5}, row_fields=small, offset=0)])
    a.apply([record("c", after={**row(2), "amount": dec("1234567890.12"), "ratio": 0.1}, row_fields=big, offset=1)])
    got = lake(sink)
    assert got[2]["amount"] == Decimal("1234567890.12") and got[2]["ratio"] == 0.1   # double, not float32-rounded
    assert got[1]["amount"] == Decimal("1.50")


@pytest.mark.parametrize("before, after", [
    (decimal_field(12, 2), decimal_field(12, 4)),        # union_by_name silently ignores a scale change
    ({"type": "string", "optional": True}, INT32),       # column dropped and re-added with another type
    ({"type": "int64", "name": "io.debezium.time.MicroTimestamp", "optional": True},
     {"type": "string", "name": "io.debezium.time.ZonedTimestamp", "optional": True}),   # timestamp -> timestamptz
])
def test_type_change_iceberg_cannot_apply_fails_clearly_and_writes_nothing(sink, before, after):
    a = Applier(sink, "lab")
    a.apply([record("c", after={**row(1), "x": None}, row_fields=fields(x=before), offset=0)])
    snap = sink.load("public.accounts").current_snapshot().snapshot_id
    # Even a NULL value is refused: the type conflict would otherwise surface later, on the first real value.
    with pytest.raises(RuntimeError, match=r"cannot apply in place .*x: lake"):
        a.apply([record("c", after={**row(2), "x": None}, row_fields=fields(x=after), offset=1)])
    t = sink.load("public.accounts")
    assert t.current_snapshot().snapshot_id == snap and set(lake(sink)) == {1}


def test_fits_and_promotable_follow_iceberg_rules():
    assert fits(pa.int16(), pa.int32()) and fits(pa.int32(), pa.int64()) and not fits(pa.int64(), pa.int32())
    assert fits(pa.string(), pa.large_string()) and fits(pa.list_(pa.string()), pa.large_list(pa.large_string()))
    assert promotable(pa.int64(), pa.int32()) and promotable(pa.float64(), pa.float32())
    assert promotable(pa.decimal128(14, 2), pa.decimal128(12, 2)) and not promotable(pa.decimal128(12, 4), pa.decimal128(12, 2))
    assert not promotable(pa.int32(), pa.string()) and not promotable(pa.timestamp("us", tz="UTC"), pa.timestamp("us"))


# ---------------------------------------------------------------- special values

INF_FIELDS = {
    "ts": {"type": "int64", "optional": True, "name": "io.debezium.time.MicroTimestamp"},
    "ts3": {"type": "int64", "optional": True, "name": "io.debezium.time.Timestamp"},
    "tsz": {"type": "string", "optional": True, "name": "io.debezium.time.ZonedTimestamp"},
    "d": {"type": "int32", "optional": True, "name": "io.debezium.time.Date"},
}
UTC = dt.timezone.utc


@pytest.mark.parametrize("name, raw, expected", [
    ("ts", 9223372036854775807, dt.datetime.max), ("ts", -9223372036854775808, dt.datetime.min),      # observed
    ("ts", 9223372036825200000, dt.datetime.max), ("ts", -9223372036832400000, dt.datetime.min),      # documented
    ("ts3", 9223372036825200000, dt.datetime.max), ("ts3", -9223372036832400000, dt.datetime.min),
    ("tsz", "infinity", dt.datetime.max.replace(tzinfo=UTC)), ("tsz", "-infinity", dt.datetime.min.replace(tzinfo=UTC)),
    ("d", 2146033979, dt.date.max), ("d", -2147472691, dt.date.min),                                  # streaming
    ("d", -622191234, dt.date.max), ("d", -625821272, dt.date.min),                                   # snapshot
    ("ts", 1704164645123456, dt.datetime(2024, 1, 2, 3, 4, 5, 123456)),
])
def test_infinity_sentinels_decode(name, raw, expected):
    assert column({**INF_FIELDS[name], "field": name}).decode(raw) == expected


def test_other_out_of_range_values_raise_instead_of_clamping():
    with pytest.raises(ValueError, match="not one of Debezium's infinity sentinels"):
        column({**INF_FIELDS["ts"], "field": "ts"}).decode(400_000 * 365 * 86_400 * 10**6)   # year ~400000


def test_infinity_round_trips_through_iceberg(sink):
    f = fields(**INF_FIELDS)
    after = {**row(1), "ts": 2**63 - 1, "ts3": -9223372036832400000, "tsz": "infinity", "d": -2147472691}
    Applier(sink, "lab").apply([record("c", after=after, row_fields=f, offset=0)])
    r = lake(sink)[1]
    assert (r["ts"], r["ts3"], r["tsz"], r["d"]) == (dt.datetime.max, dt.datetime.min,
                                                     dt.datetime.max.replace(tzinfo=UTC), dt.date.min)


# ---------------------------------------------------------------- arrays

ARRAY_FIELDS = {
    "tags": {"type": "array", "items": {"type": "string", "optional": True}, "optional": True},
    "amounts": {"type": "array", "items": {**decimal_field(8, 2)}, "optional": True},
    "blob": {"type": "array", "items": {"type": "string", "optional": True}, "optional": True},
}


def test_array_columns_decode_and_their_toast_placeholder_is_filled(sink):
    f = fields(**ARRAY_FIELDS)
    big = [f"x{i}" * 30 for i in range(200)]
    a = Applier(sink, "lab")
    a.apply([record("c", after={**row(1), "tags": ["a", None], "amounts": [dec("1.50"), None, dec("-0.01")],
                                 "blob": big}, row_fields=f, offset=0, seq=(1, 1))])
    upd = record("u", after={**row(1, name="b"), "tags": [], "amounts": None, "blob": [TOAST_PLACEHOLDER]},
                 row_fields=f, offset=1, seq=(2, 2))
    ev = parse(*upd)
    assert ev.unavailable == {"blob"}           # through parse(), not just the helper
    a.apply([upd])
    r = lake(sink)[1]
    assert r["name"] == "b" and r["tags"] == [] and r["amounts"] is None and r["blob"] == big
    first = parse(*record("c", after={**row(2), "tags": None, "amounts": [dec("2.25")], "blob": None},
                          row_fields=f, offset=2))
    assert first.after["amounts"] == [Decimal("2.25")]


# ---------------------------------------------------------------- replay filter and batching

def test_replay_that_starts_inside_a_batch_is_dropped(sink):
    """Connect re-emits c(1)@pos100 and u(1)@pos200 after a crash; a lagging consumer sees the genuine pair and the
    first replayed event in ONE batch. Compaction keeps the last event per key, so without an in-batch check the
    replayed (older) create would win, and the later replay of the update would then be dropped."""
    a = Applier(sink, "lab")
    a.apply([record("c", after=row(1, notes="v1"), offset=0, seq=(10, 100)),
             record("u", after=row(1, notes="v2"), offset=1, seq=(20, 200)),
             record("c", after=row(1, notes="v1"), offset=2, seq=(10, 100))])    # replay begins
    a.apply([record("u", after=row(1, notes="v2"), offset=3, seq=(20, 200))])
    assert lake(sink)[1]["notes"] == "v2" and a.skipped_replays == 2


def newkey_delete(offset, old, new, seq):
    k, v, t, p, o, _ = record("d", before=row(old), offset=offset, seq=seq)
    return k, v, t, p, o, [("__debezium.newkey", json.dumps({"payload": {"id": new}}).encode())]


def test_hold_unpaired_deletes_per_partition():
    d = newkey_delete(5, 1, 101, (70, 700))
    other = record("c", after=row(9), offset=6, partition=1)
    assert hold_unpaired_deletes([record("c", after=row(3), offset=4), d, other]) == \
        ([record("c", after=row(3), offset=4)], [d, other])     # another partition's later record does not hide it
    c = record("c", after=row(101), offset=6, old_key={"id": 1}, seq=(70, 700))
    assert hold_unpaired_deletes([d, c]) == ([d, c], [])


class FakeMessage:
    def __init__(self, r):
        self.r = r

    def key(self): return self.r[0]
    def value(self): return self.r[1]
    def topic(self): return self.r[2]
    def partition(self): return self.r[3]
    def offset(self): return self.r[4]
    def headers(self): return self.r[5] if len(self.r) > 5 else None
    def error(self): return None


class FakeConsumer:
    """Plays back scripted polls through the real run() loop."""

    def __init__(self, polls, partitions=((T, 0),)):
        self.polls, self.partitions = list(polls), partitions
        self.assigned, self.commits, self.on_assign = None, [], None

    def subscribe(self, topics, on_assign):
        self.on_assign = on_assign

    def assign(self, partitions):
        self.assigned = [(tp.topic, tp.partition, tp.offset) for tp in partitions]

    def consume(self, num_messages, timeout):
        if self.assigned is None:
            from confluent_kafka import TopicPartition
            self.on_assign(self, [TopicPartition(t, p) for t, p in self.partitions])
        return [FakeMessage(r) for r in self.polls.pop(0)] if self.polls else []

    def commit(self, offsets, asynchronous):
        self.commits.append({(tp.topic, tp.partition): tp.offset for tp in offsets})

    def close(self):
        pass


def drive(sink, polls, batch, **kw):
    fake = FakeConsumer(polls, **kw)
    run("unused", "lab", [T], sink, batch=batch, idle_exit_s=0.3, consumer_factory=lambda cfg: fake)
    return fake


def test_full_poll_never_commits_a_pk_change_delete_without_its_create(sink):
    """UPDATE accounts SET id = id + 100 in one transaction: d(1) c(101) d(2) c(102), each pair sharing a position.
    With --batch 3 the first poll ends right after d(2). Applying d(2) alone would store its position, and c(102)
    (same position) would then be dropped as a replay: row 102 silently lost."""
    drive(sink, [[record("c", after=row(1), offset=0, tx=8, seq=(60, 600)),
                  record("c", after=row(2), offset=1, tx=8, seq=(60, 610))]], batch=10)
    tx = [newkey_delete(2, 1, 101, (70, 700)),
          record("c", after=row(101), offset=3, tx=1, old_key={"id": 1}, seq=(70, 700)),
          newkey_delete(4, 2, 102, (70, 710)),
          record("c", after=row(102), offset=5, tx=1, old_key={"id": 2}, seq=(70, 710))]
    fake = drive(sink, [tx[:3], tx[3:]], batch=3)
    assert set(lake(sink)) == {101, 102}
    assert fake.assigned == [(T, 0, 2)]                         # seeked to stored offset + 1
    assert fake.commits[-1] == {(T, 0): 6}


def test_new_topic_seeks_to_beginning_and_crash_resume_seeks_past_stored_offset(sink):
    from confluent_kafka import OFFSET_BEGINNING
    fake = drive(sink, [[record("c", after=row(1), offset=0), record("c", after=row(2), offset=1)]], batch=10)
    assert fake.assigned == [(T, 0, OFFSET_BEGINNING)]
    fake = drive(sink, [], batch=10)
    assert fake.assigned == [(T, 0, 2)]


# ---------------------------------------------------------------- fencing (real conflicts, no monkeypatching)

def test_empty_commits_are_fenced_against_a_concurrent_writer(sink):
    a = Applier(sink, "lab")
    a.apply([record("c", after=row(1), offset=0)])
    stale = sink.load("public.accounts")
    a.apply([record("c", after=row(2), offset=1)])               # another writer moves the table on
    with pytest.raises(CommitFailedException):
        sink.record_state("public.accounts", {(T, 0): 0}, table=stale)
    plan = compact([parse(*record("d", before=row(99), offset=0))])["public.accounts"]
    with pytest.raises(CommitFailedException):
        sink.apply(plan, {(T, 0): 0}, table=stale)               # delete-only plan matching nothing
    assert sink.stored_offsets("public.accounts")[(T, 0)] == 1 and set(lake(sink)) == {1, 2}


# ---------------------------------------------------------------- reconcile

def test_reconcile_compares_exactly_and_handles_non_finite_values():
    assert normalise(Decimal("NaN")) == normalise(Decimal("NaN")) and normalise(float("nan")) == normalise(float("nan"))
    assert normalise(Decimal("1.50")) == normalise(Decimal("1.5"))
    d = compare("t", ["id"], [{"id": 1, "v": 1.5e-12}], [{"id": 1, "v": -3.0e-12}])
    assert not d.match                                            # no absolute tolerance hiding small doubles
    lake_real = pa.array([0.1], pa.float32()).to_pylist()[0]      # what the lake returns for a real 0.1
    assert compare("t", ["id"], [{"id": 1, "v": _float32(0.1)}], [{"id": 1, "v": lake_real}]).match
    assert _float32([0.1, None]) == [lake_real, None]


# ---------------------------------------------------------------- fixtures themselves

def test_decimal_fixture_round_trips():
    d = _decimal_decoder(2)
    for v in ("-0.01", "123.45", "327.68", "-327.68", "0.00", "9999999999.99"):
        assert d(dec(v)) == Decimal(v)
