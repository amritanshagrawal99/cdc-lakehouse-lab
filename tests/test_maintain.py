"""Table maintenance against a real Iceberg catalog, interleaved with the real Applier."""
import datetime as dt
import os
import time

import pytest
from debezium_fixtures import record, row
from pyiceberg.catalog.sql import SqlCatalog

from cdclab.consumer import Applier, stored_state
from cdclab.maintain import MaintenanceReport, _local, compact, maintain, referenced_files
from cdclab.sink import IcebergSink

T = "public.accounts"


@pytest.fixture
def sink(tmp_path):
    wh = tmp_path.as_posix()
    return IcebergSink(SqlCatalog("lab", uri=f"sqlite:///{wh}/cat.db", warehouse=wh))


def lake(sink):
    return {r["id"]: r for r in sink.load(T).scan().to_arrow().to_pylist()}


def batches(sink, n, start=0):
    """n consumer batches, each touching keys 0..2 (copy-on-write rewrites) and adding one new key."""
    a = Applier(sink, "lab")
    for i in range(start, start + n):
        a.apply([record("u", after=row(k, name=f"b{i}"), offset=4 * i + k, seq=(10 * i + 1, 10 * i + k + 1))
                 for k in range(3)] + [record("c", after=row(100 + i), offset=4 * i + 3, seq=(10 * i + 1, 10 * i + 4))])
    return a


def age_all_files(sink, minutes=120):
    """Make every file in the table look old enough for orphan cleanup."""
    old = time.time() - minutes * 60
    for root, _, files in os.walk(_local(sink.load(T).location())):
        for f in files:
            os.utime(os.path.join(root, f), (old, old))


def test_full_maintenance_keeps_data_offsets_and_positions(sink):
    batches(sink, 40)
    before_rows, before_state = lake(sink), stored_state(sink.load(T).properties)
    assert sink.load(T).inspect.files().num_rows >= 32
    age_all_files(sink)
    r = maintain(sink, T, retain_hours=0, keep_last=2, compact_min_files=32, grace_minutes=60)
    t = sink.load(T)
    assert r.compacted and r.data_files_after == 1 and t.inspect.files().num_rows == 1
    assert r.snapshots_expired > 30 and len(t.snapshots()) <= 3
    assert r.orphans and not any(os.path.exists(p) for p in r.orphans)
    assert lake(sink) == before_rows
    assert stored_state(t.properties) == before_state           # offsets + positions live in properties
    assert t.current_snapshot().summary.additional_properties.get("cdclab.maintenance") == "compaction"


@pytest.mark.parametrize("compact_min_files", [32, 1000])   # 1000: no compaction, so even the current files are old
def test_every_referenced_file_survives_and_the_consumer_continues(sink, compact_min_files):
    batches(sink, 35)
    age_all_files(sink)
    maintain(sink, T, retain_hours=0, keep_last=1, compact_min_files=compact_min_files, grace_minutes=60)
    t = sink.load(T)
    # Checked on disk, independently of referenced_files(): pyiceberg caches manifest lists in memory, so a
    # deleted one would not fail reads in this process, only in the next one.
    needed = [t.metadata_location] + [s.manifest_list for s in t.snapshots()]
    needed += t.inspect.all_manifests().column("path").to_pylist() + t.inspect.all_files().column("file_path").to_pylist()
    assert not [p for p in needed if not _local(p).exists()]
    assert not [p for p in referenced_files(t) if not p.exists()]
    batches(sink, 3, start=35)                                   # the consumer keeps going on the maintained table
    assert len(lake(sink)) == 3 + 38 and lake(sink)[0]["name"] == "b37"


def test_compaction_built_on_a_stale_view_is_rejected_not_applied(sink):
    batches(sink, 33)
    stale = sink.load(T)
    batches(sink, 1, start=33)                                   # a consumer commit lands in between
    r = MaintenanceReport(T, dry_run=False)
    compact(stale, min_files=32, max_bytes=1 << 40, dry_run=False, report=r)
    assert not r.compacted and "consumer committed" in r.compaction_skipped
    assert 133 in lake(sink) and lake(sink)[0]["name"] == "b33"   # the consumer's batch is still there


def test_young_unreferenced_files_are_kept(sink):
    batches(sink, 5)
    stray = _local(sink.load(T).location()) / "data" / "in-flight-commit.parquet"
    stray.write_bytes(b"not yet committed")
    r = maintain(sink, T, retain_hours=0, keep_last=1, compact_min_files=1000, grace_minutes=60)
    assert stray.exists() and r.young_unreferenced >= 1 and str(stray) not in r.orphans
    old = time.time() - 7200
    os.utime(stray, (old, old))
    r = maintain(sink, T, retain_hours=0, keep_last=1, compact_min_files=1000, grace_minutes=60)
    assert not stray.exists() and str(stray.resolve()) in {os.path.realpath(p) for p in r.orphans}


def test_dry_run_changes_nothing(sink):
    batches(sink, 34)
    age_all_files(sink)
    t = sink.load(T)
    files_before = sorted(str(p) for p in _local(t.location()).rglob("*") if p.is_file())
    r = maintain(sink, T, retain_hours=0, keep_last=1, dry_run=True)
    t2 = sink.load(T)
    assert r.dry_run and not r.compacted and r.snapshots_expired > 0
    assert len(t2.snapshots()) == len(t.snapshots()) and t2.metadata_location == t.metadata_location
    assert sorted(str(p) for p in _local(t2.location()).rglob("*") if p.is_file()) == files_before


def test_retention_keeps_recent_snapshots(sink):
    batches(sink, 5)
    r = maintain(sink, T, retain_hours=24, keep_last=1, compact_min_files=1000)
    assert r.snapshots_expired == 0 and len(sink.load(T).snapshots()) == r.snapshots_before


def test_local_paths():
    assert _local("file:///C:/lake/x.parquet").as_posix().endswith("lake/x.parquet")
    assert _local("/tmp/lake/x").name == "x"
    with pytest.raises(ValueError, match="local warehouses only"):
        _local("s3://bucket/lake/x.parquet")
    assert dt.timedelta(minutes=1)


def test_keep_last_n_snapshots_even_past_retention(sink):
    batches(sink, 8)
    r = maintain(sink, T, retain_hours=0, keep_last=5, compact_min_files=1000)
    assert len(sink.load(T).snapshots()) == 5 and r.snapshots_expired == r.snapshots_before - 5
