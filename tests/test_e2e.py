"""End to end against the real stack: Postgres -> Debezium -> Kafka -> Iceberg.

Needs `docker compose up -d` (see the CI workflow); skipped unless CDCLAB_E2E=1. Each step runs the real CLI in a
subprocess, so the crash hook (os._exit after an Iceberg commit, before the Kafka commit) kills only that consumer.
"""
import os
import re
import subprocess
import sys

import psycopg
import pytest

pytestmark = pytest.mark.skipif(os.environ.get("CDCLAB_E2E") != "1", reason="set CDCLAB_E2E=1 with the stack running")
DSN = "postgresql://lab:lab@localhost:55432/lab"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def cdclab(*args, lake, env=None, check=True):
    cmd = [sys.executable, "-m", "cdclab.cli", "--lake", str(lake), *args]
    r = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, env={**os.environ, "PYTHONUTF8": "1", **(env or {})})
    if check and r.returncode != 0:
        raise AssertionError(f"{' '.join(args)} -> {r.returncode}\n{r.stdout[-3000:]}\n{r.stderr[-3000:]}")
    return r


TABLES = ("public.accounts", "public.orders", "public.edge_types")


def assert_all_match(r):
    """Every table on its own `MATCH  <table>` line, and no MISMATCH anywhere ('MISMATCH ' contains 'MATCH ')."""
    assert r.returncode == 0 and "MISMATCH" not in r.stdout, r.stdout
    assert sorted(re.findall(r"^MATCH  (\S+):", r.stdout, re.M)) == sorted(TABLES), r.stdout


def insert_infinities(first_id):
    with psycopg.connect(DSN, autocommit=True) as c:
        for i, inf in enumerate(("infinity", "-infinity")):
            c.execute("INSERT INTO public.edge_types (id, valid_from, valid_to, closed_at, seen_at) "
                      "VALUES (%s, %s, %s, %s, %s)", (first_id + i, inf, inf, inf, inf))


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS public.accounts, public.orders, public.edge_types")
    path = tmp_path_factory.mktemp("lake")
    cdclab("setup", lake=path)
    cdclab("workload", "--steps", "80", "--seed", "1", "--schema-change-at", "-1", lake=path)  # rows before the snapshot
    insert_infinities(1_000_001)   # Debezium encodes some of these differently in the snapshot than when streaming
    cdclab("register", lake=path)
    return path


def test_nan_is_rejected_at_the_source(lake):
    """A NUMERIC NaN would kill the Debezium task (precise decimal mode); the lab keeps it out with a CHECK."""
    with psycopg.connect(DSN, autocommit=True) as c, pytest.raises(psycopg.errors.CheckViolation):
        c.execute("INSERT INTO public.accounts (id, name, balance, notes) VALUES (900001, 'nan', 'NaN', 'x')")


def test_snapshot_then_streaming_matches(lake):
    cdclab("workload", "--steps", "300", "--seed", "2", "--schema-change-at", "150", lake=lake)
    insert_infinities(2_000_001)   # the same values, streamed
    cdclab("consume", "--idle-exit", "20", "--batch", "25", lake=lake)
    r = cdclab("reconcile", lake=lake)
    assert_all_match(r)
    # The edge cases really happened: the type widenings were evolved, and infinity / arrays / TOAST reached the lake.
    with psycopg.connect(DSN) as c:
        (inf,) = c.execute("SELECT count(*) FROM public.edge_types WHERE valid_to = 'infinity'").fetchone()
        (big,) = c.execute("SELECT count(*) FROM public.edge_types WHERE counter > 2147483647").fetchone()
        (toast,) = c.execute("SELECT count(*) FROM public.edge_types WHERE cardinality(blob) > 1").fetchone()
    assert inf and big and toast, (inf, big, toast)
    from cdclab.cli import catalog
    schema = catalog(str(lake)).load_table("cdc.public__edge_types").schema()
    assert (str(schema.find_field("counter").field_type), str(schema.find_field("amount").field_type)) ==         ("long", "decimal(12, 2)")


def test_consumer_crashes_and_resumes_exactly_once(lake):
    cdclab("workload", "--steps", "300", "--seed", "3", "--schema-change-at", "-1", lake=lake)
    for crash_after in (2, 5, 9):
        r = cdclab("consume", "--idle-exit", "20", "--batch", "9", lake=lake,
                   env={"CDCLAB_CRASH_AFTER_COMMITS": str(crash_after)}, check=False)
        assert r.returncode == 17, (r.returncode, r.stderr[-2000:])   # died between the Iceberg and Kafka commits
    cdclab("consume", "--idle-exit", "20", "--batch", "9", lake=lake)   # resume: partitions seek to lake offsets
    r = cdclab("reconcile", lake=lake)
    assert_all_match(r)


def test_kafka_connect_crash_replays_are_absorbed(lake):
    """SIGKILL Kafka Connect while the workload runs: Debezium resumes from its last flushed offset and re-emits
    events at new Kafka offsets. The lake must still match the source exactly."""
    import time
    work = subprocess.Popen([sys.executable, "-m", "cdclab.cli", "--lake", str(lake), "workload", "--steps", "400",
                             "--seed", "4", "--schema-change-at", "-1"], cwd=ROOT, env={**os.environ, "PYTHONUTF8": "1"})
    time.sleep(4)
    subprocess.run(["docker", "compose", "kill", "-s", "SIGKILL", "connect"], cwd=ROOT, check=True, capture_output=True)
    subprocess.run(["docker", "compose", "start", "connect"], cwd=ROOT, check=True, capture_output=True)
    assert work.wait(timeout=600) == 0
    cdclab("register", lake=lake)   # waits until the connector (restarted with the worker) is RUNNING again
    r = cdclab("consume", "--idle-exit", "25", "--batch", "50", lake=lake)
    print(r.stderr[-1500:])
    r = cdclab("reconcile", lake=lake)
    assert_all_match(r)
