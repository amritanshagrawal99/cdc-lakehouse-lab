"""Topics with several partitions. Debezium routes every event by its key, so the old and new key of a primary-key
change usually land in DIFFERENT partitions. Found on the live stack with 2 partitions: the consumer failed on its
first batch, on every retry, because compaction ran in (partition, offset) order."""
import json

import pytest
from debezium_fixtures import record, row
from pyiceberg.catalog.sql import SqlCatalog
from test_review_v2 import FakeConsumer

from cdclab.consumer import Applier, hold_unpaired_deletes, run
from cdclab.events import TOAST_PLACEHOLDER
from cdclab.sink import IcebergSink

T = "lab.public.accounts"
BIG = "x" * 9000


@pytest.fixture
def sink(tmp_path):
    wh = tmp_path.as_posix()
    return IcebergSink(SqlCatalog("lab", uri=f"sqlite:///{wh}/cat.db", warehouse=wh))


def lake(sink):
    t = sink.load("public.accounts")
    return {r["id"]: r for r in t.scan().to_arrow().to_pylist()} if t else {}


def pk_delete(old, new, partition, offset, seq):
    k, v, t, p, o, _ = record("d", before=row(old), partition=partition, offset=offset, seq=seq)
    return k, v, t, p, o, [("__debezium.newkey", json.dumps({"schema": {}, "payload": {"id": new}}).encode())]


def pk_create(old, new, partition, offset, seq, notes=TOAST_PLACEHOLDER):
    return record("c", after=row(new, notes=notes), partition=partition, offset=offset, seq=seq, old_key={"id": old})


def test_pk_change_across_partitions_in_one_batch(sink):
    """Row 1 is inserted (partition 1), then its key changes to 7: delete(1) stays in partition 1, create(7) lands in
    partition 0 and copies the unchanged TOAST value from row 1. In (partition, offset) order the create came first,
    before row 1 existed anywhere."""
    batch = [pk_create(1, 7, partition=0, offset=0, seq=(20, 200)),
             record("c", after=row(1, notes=BIG), partition=1, offset=0, seq=(10, 100)),
             pk_delete(1, 7, partition=1, offset=1, seq=(20, 200))]
    Applier(sink, "lab").apply(batch)
    assert set(lake(sink)) == {7} and lake(sink)[7]["notes"] == BIG


def test_interleaved_updates_of_different_keys_apply_in_commit_order(sink):
    a = Applier(sink, "lab")
    a.apply([record("c", after=row(1, notes=BIG), partition=1, offset=0, seq=(10, 100))])
    # key 1 -> 7 (create half in partition 0), then key 7 is updated in partition 0, in the same batch.
    a.apply([pk_create(1, 7, partition=0, offset=0, seq=(20, 200)),
             record("u", after=row(7, name="later", notes=TOAST_PLACEHOLDER), partition=0, offset=1, seq=(30, 300)),
             pk_delete(1, 7, partition=1, offset=1, seq=(20, 200))])
    r = lake(sink)
    assert set(r) == {7} and r[7]["name"] == "later" and r[7]["notes"] == BIG


def test_hold_matches_halves_by_key_not_partition():
    d = pk_delete(1, 7, partition=1, offset=5, seq=(20, 200))
    c = pk_create(1, 7, partition=0, offset=9, seq=(20, 200))
    other = record("c", after=row(3), partition=1, offset=6, seq=(30, 300))
    assert hold_unpaired_deletes([d, other]) == ([], [d, other])          # create not seen yet: hold
    assert hold_unpaired_deletes([c, d, other]) == ([c, d, other], [])    # create seen first, in another partition
    assert hold_unpaired_deletes([d, other, c]) == ([d, other, c], [])
    unrelated = pk_create(2, 8, partition=0, offset=9, seq=(20, 200))     # a create for a different old key
    assert hold_unpaired_deletes([d, unrelated])[1] == [d, unrelated]


def test_pair_split_across_polls_and_partitions_through_run(sink):
    """The delete half arrives in partition 1 in one poll; its create in partition 0 only in the next. The delete is
    held, both halves are applied together, and the TOAST value is copied from the old row."""
    first = drive(sink, [[record("c", after=row(1, notes=BIG), partition=1, offset=0, tx=5, seq=(10, 100))]],
                  partitions=((T, 0), (T, 1)))
    assert set(lake(sink)) == {1} and first
    drive(sink, [[pk_delete(1, 7, partition=1, offset=1, seq=(20, 200))],
                 [pk_create(1, 7, partition=0, offset=0, seq=(20, 200))]], partitions=((T, 0), (T, 1)))
    assert set(lake(sink)) == {7} and lake(sink)[7]["notes"] == BIG


def drive(sink, polls, partitions):
    fake = FakeConsumer(polls, partitions=partitions)
    run("unused", "lab", [T], sink, batch=100, idle_exit_s=0.3, consumer_factory=lambda cfg: fake)
    return fake


def test_create_half_waits_for_a_lagging_old_key_partition(sink):
    """The live failure, replayed: partition 1 delivers create(95, oldkey 34) before partition 0 has delivered the
    insert of key 34 at all. Applied alone, the create had no old row to copy its TOAST value from."""
    drive(sink, [[pk_create(34, 95, partition=1, offset=142, seq=(30, 300))],
                 [record("c", after=row(34, notes=BIG), partition=0, offset=24, tx=885, seq=(10, 100)),
                  pk_delete(34, 95, partition=0, offset=118, seq=(30, 300))]], partitions=((T, 0), (T, 1)))
    assert set(lake(sink)) == {95} and lake(sink)[95]["notes"] == BIG


def test_create_alone_is_held():
    c = pk_create(34, 95, partition=1, offset=142, seq=(30, 300))
    assert hold_unpaired_deletes([c]) == ([], [c])


def test_batch_is_closed_under_pairing():
    """Live failure #2: both halves of A (34 -> 95) were in the list, but the cut for an unrelated unpaired half B
    fell between them, so the batch kept A's create and held A's delete."""
    a_create = pk_create(34, 95, partition=1, offset=142, seq=(30, 300))
    b_delete = pk_delete(50, 60, partition=1, offset=150, seq=(40, 400))      # its create has not arrived
    filler = record("c", after=row(3), partition=0, offset=100, seq=(35, 350))
    a_delete = pk_delete(34, 95, partition=0, offset=118, seq=(30, 300))
    records = [a_create, b_delete, filler, a_delete]
    ready, held = hold_unpaired_deletes(records)
    assert ready == [] and held == records         # cutting at b_delete would have kept a_create alone
    assert hold_unpaired_deletes([a_create, filler, a_delete, b_delete]) == ([a_create, filler, a_delete], [b_delete])


def test_a_replayed_half_whose_partner_was_already_applied_does_not_stall(sink):
    """Live failure #3, after a Kafka Connect SIGKILL: both halves of 35 -> 65 exist twice (original + replay). The
    replayed create (partition 1) arrived early and was consumed with the original pair; the replayed delete
    (partition 0) then had no partner left and held back every later record, in every table."""
    # Empty polls between batches (an idle gap) so each batch is applied before the next one arrives.
    fake = drive(sink, [
        [record("c", after=row(35, notes=BIG), partition=0, offset=0, seq=(10, 100))], [],
        [pk_delete(35, 65, partition=0, offset=1, seq=(20, 200)),
         pk_create(35, 65, partition=1, offset=0, seq=(20, 200)),
         pk_create(35, 65, partition=1, offset=1, seq=(20, 200))], [],             # the create's replay came early
        [pk_delete(35, 65, partition=0, offset=2, seq=(20, 200))], [],             # the delete's replay, alone
        [record("u", after=row(65, name="after", notes=TOAST_PLACEHOLDER), partition=1, offset=2, seq=(30, 300)),
         record("c", after=row(70), partition=0, offset=3, seq=(40, 400))]], partitions=((T, 0), (T, 1)))
    r = lake(sink)
    assert set(r) == {65, 70} and r[65]["name"] == "after" and r[65]["notes"] == BIG
    committed = {}
    for c in fake.commits:
        committed.update(c)
    assert committed == {(T, 0): 4, (T, 1): 3}       # every record applied and committed: nothing left held


def test_halves_pair_by_key_and_position():
    d = pk_delete(35, 65, partition=0, offset=1, seq=(20, 200))
    other_change = pk_create(35, 99, partition=1, offset=5, seq=(50, 500))     # a LATER key change of key 35
    assert hold_unpaired_deletes([d, other_change])[0] == []                   # not d's partner
