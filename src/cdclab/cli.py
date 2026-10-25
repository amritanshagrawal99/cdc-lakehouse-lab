"""`cdclab` command line: register the connector, run the workload, consume into Iceberg, reconcile."""
from __future__ import annotations

import argparse
import http.client
import json
import logging
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

DSN = "postgresql://lab:lab@localhost:55432/lab"
TABLES = ["public.accounts", "public.orders", "public.edge_types"]


def catalog(lake_dir: str):
    from pyiceberg.catalog.sql import SqlCatalog
    wh = Path(lake_dir).resolve()
    wh.mkdir(parents=True, exist_ok=True)
    return SqlCatalog("lab", uri=f"sqlite:///{wh.as_posix()}/catalog.db", warehouse=wh.as_posix())


def _http(method: str, url: str, body: dict | None = None):
    req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read() or b"null")


def register(connect_url: str, config_path: str, timeout_s: int = 180) -> None:
    cfg = json.loads(Path(config_path).read_text())
    name = cfg["name"]
    deadline = time.monotonic() + timeout_s
    # Wait for the worker to be READY (KIP-1017 /health), not merely listening: after a restart the REST API
    # answers early with 404 / 409 / dropped connections while the worker rejoins its group.
    while True:
        try:
            _http("GET", f"{connect_url}/health")
            break
        except (OSError, http.client.HTTPException):
            if time.monotonic() > deadline:
                raise SystemExit(f"Kafka Connect at {connect_url} not healthy after {timeout_s}s") from None
            time.sleep(2)
    while True:
        try:
            _http("PUT", f"{connect_url}/connectors/{name}/config", cfg["config"])
            break
        except urllib.error.HTTPError as e:
            # 409 / 5xx while the worker is (re)joining its group after a restart: retry until the deadline.
            if e.code not in (404, 409, 500, 503) or time.monotonic() > deadline:
                raise SystemExit(f"connector registration failed: {e.code} {e.read().decode()[:500]}") from e
        except (OSError, http.client.HTTPException):   # refused / reset / RemoteDisconnected while the worker restarts
            if time.monotonic() > deadline:
                raise
        time.sleep(2)
    while time.monotonic() < deadline:
        try:
            st = _http("GET", f"{connect_url}/connectors/{name}/status")
            tasks = st.get("tasks", [])
            if st["connector"]["state"] == "RUNNING" and tasks and all(t["state"] == "RUNNING" for t in tasks):
                print(f"connector {name} RUNNING")
                return
            failed = [t for t in tasks if t["state"] == "FAILED"]
            if failed:
                raise SystemExit(f"connector task failed: {failed[0].get('trace', '')[:2000]}")
        except (OSError, http.client.HTTPException):   # HTTPError is an OSError too
            pass
        time.sleep(2)
    raise SystemExit(f"connector {name} not RUNNING after {timeout_s}s")


def cmd_register(a) -> int:
    register(a.connect, a.config)
    return 0


def cmd_setup(a) -> int:
    from .workload import setup
    setup(a.dsn)
    print("source tables created")
    return 0


def cmd_workload(a) -> int:
    from .workload import run
    print(run(a.dsn, steps=a.steps, seed=a.seed, schema_change_at=a.schema_change_at))
    return 0


def cmd_consume(a) -> int:
    from .consumer import run
    from .sink import IcebergSink
    topics = [f"{a.prefix}.{t}" for t in TABLES]
    n = run(a.bootstrap, a.prefix, topics, IcebergSink(catalog(a.lake)), group=a.group, batch=a.batch, idle_exit_s=a.idle_exit)
    print(f"{n} Iceberg commits")
    return 0


def cmd_reconcile(a) -> int:
    import psycopg

    from .reconcile import compare, lake_rows, primary_key, source_rows
    from .sink import IcebergSink
    sink = IcebergSink(catalog(a.lake))
    ok = True
    with psycopg.connect(a.dsn) as conn:
        for table in TABLES:
            names, src = source_rows(conn, table)
            t = sink.load(table)
            d = compare(table, primary_key(conn, table), src, lake_rows(t) if t else [], names)
            print(d.describe())
            ok &= d.match
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    p = argparse.ArgumentParser(prog="cdclab", description="Postgres -> Debezium -> Kafka -> Iceberg, exactly once.")
    p.add_argument("--dsn", default=DSN)
    p.add_argument("--lake", default="lake", help="local Iceberg warehouse directory")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("setup", help="create the source tables")
    s.set_defaults(func=cmd_setup)
    r = sub.add_parser("register", help="register the Debezium connector and wait until it runs")
    r.add_argument("--connect", default="http://localhost:18083")
    r.add_argument("--config", default="connector.json")
    r.set_defaults(func=cmd_register)
    w = sub.add_parser("workload", help="run the change workload against Postgres")
    w.add_argument("--steps", type=int, default=300)
    w.add_argument("--seed", type=int, default=7)
    w.add_argument("--schema-change-at", type=int, default=150)
    w.set_defaults(func=cmd_workload)
    c = sub.add_parser("consume", help="apply Kafka change events to Iceberg")
    c.add_argument("--bootstrap", default="localhost:29092")
    c.add_argument("--prefix", default="lab")
    c.add_argument("--group", default="cdclab")
    c.add_argument("--idle-exit", type=float, default=None, help="stop after this many idle seconds")
    c.add_argument("--batch", type=int, default=500, help="max records per micro-batch")
    c.set_defaults(func=cmd_consume)
    rc = sub.add_parser("reconcile", help="compare every source table with its Iceberg copy")
    rc.set_defaults(func=cmd_reconcile)
    a = p.parse_args(argv)
    return a.func(a)


if __name__ == "__main__":
    sys.exit(main())
