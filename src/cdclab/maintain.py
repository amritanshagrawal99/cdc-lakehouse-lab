"""Table maintenance that is safe to run while the consumer is writing.

Each consumer batch adds a snapshot, and copy-on-write deletes rewrite every file holding a touched key, so a CDC
table collects small files, snapshots and unreferenced files fast. In order:

1. compact     rewrite the table as one overwrite when it has many data files. It reads snapshot S and commits
               only if the table is still at S: a consumer commit in between makes it fail (verified), and this run
               just skips it. Never retried in a loop: an active consumer would keep winning, which is fine.
2. expire      drop snapshots older than the retention, always keeping the current one and the newest N. The
               consumer's offsets and positions are table properties, not snapshot data, so they are untouched;
               a consumer commit built on a pre-expiry view still succeeds (verified).
3. orphans     delete files under the table location that no remaining snapshot or metadata file references.
               pyiceberg's expiry deletes nothing itself (verified), so this is where space comes back. Files
               younger than the grace period are kept: they may belong to a commit in flight.

Run it on a schedule (e.g. hourly) with --dry-run first.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import unquote, urlparse

from pyiceberg.exceptions import CommitFailedException
from pyiceberg.table import Table

log = logging.getLogger("cdclab")


@dataclass
class MaintenanceReport:
    table: str
    dry_run: bool
    data_files_before: int = 0
    data_files_after: int = 0
    compacted: bool = False
    compaction_skipped: str | None = None
    snapshots_before: int = 0
    snapshots_expired: int = 0
    orphans: list[str] = field(default_factory=list)
    orphan_bytes: int = 0
    young_unreferenced: int = 0

    def describe(self) -> str:
        verb = "would" if self.dry_run else ""
        parts = [f"{self.table}: data files {self.data_files_before} -> {self.data_files_after}"
                 + (" (compacted)" if self.compacted else f" ({self.compaction_skipped})" if self.compaction_skipped else ""),
                 f"snapshots {self.snapshots_before}, {verb + ' ' if verb else ''}expired {self.snapshots_expired}",
                 f"orphan files {len(self.orphans)} ({self.orphan_bytes / 2**20:.1f} MiB) "
                 f"{'found' if self.dry_run else 'deleted'}"
                 + (f", {self.young_unreferenced} newer than the grace period kept" if self.young_unreferenced else "")]
        return "; ".join(parts)


def _local(uri: str) -> Path:
    """file:///C:/x, file:/x or a plain path -> a local Path (the lab's warehouse is a local directory)."""
    u = urlparse(uri)
    if u.scheme in ("", "file") or len(u.scheme) == 1:          # len 1: a Windows drive letter parsed as a scheme
        path = unquote(u.path) if u.scheme == "file" else uri
        if u.scheme == "file" and len(path) > 2 and path[0] == "/" and path[2] == ":":
            path = path[1:]                                       # /C:/x -> C:/x
        return Path(path).resolve()
    raise ValueError(f"orphan cleanup supports local warehouses only, not {u.scheme}:// ({uri})")


def compact(table: Table, min_files: int, max_bytes: int, dry_run: bool, report: MaintenanceReport) -> Table:
    files = table.inspect.files()
    report.data_files_before = report.data_files_after = files.num_rows
    if files.num_rows < min_files:
        report.compaction_skipped = f"under {min_files} files"
        return table
    size = sum(files.column("file_size_in_bytes").to_pylist())
    if size > max_bytes:
        report.compaction_skipped = (f"{size / 2**30:.1f} GiB is too much for a whole-table rewrite here; "
                                     "use Spark's rewrite_data_files")
        return table
    if dry_run:
        report.compaction_skipped = "dry run"
        return table
    data = table.scan().to_arrow()          # snapshot S
    try:
        with table.transaction() as tx:     # commits only if the table is still at S
            tx.overwrite(data, snapshot_properties={"cdclab.maintenance": "compaction"})
    except CommitFailedException:
        report.compaction_skipped = "a consumer committed meanwhile; skipped this run"
        return table.refresh()
    table = table.refresh()
    report.compacted = True
    report.data_files_after = table.inspect.files().num_rows
    return table


def expire(table: Table, retain: dt.timedelta, keep_last: int, dry_run: bool, report: MaintenanceReport) -> Table:
    for _ in range(3):
        snaps = sorted(table.snapshots(), key=lambda s: s.timestamp_ms)
        report.snapshots_before = len(snaps)
        cutoff = (dt.datetime.now(dt.timezone.utc) - retain).timestamp() * 1000
        current = table.current_snapshot().snapshot_id if table.current_snapshot() else None
        protected = {s.snapshot_id for s in snaps[-keep_last:]} | {current} | {r.snapshot_id for r in table.refs().values()}
        doomed = [s.snapshot_id for s in snaps if s.timestamp_ms < cutoff and s.snapshot_id not in protected]
        report.snapshots_expired = len(doomed)
        if dry_run or not doomed:
            return table
        try:
            table.maintenance.expire_snapshots().by_ids(doomed).commit()
            return table.refresh()
        except CommitFailedException:
            table = table.refresh()         # a consumer commit landed: recompute from the new head
    log.warning("%s: snapshot expiry kept conflicting with the consumer; try again later", report.table)
    return table


def referenced_files(table: Table) -> set[Path]:
    refs = {_local(p) for p in table.inspect.all_files().column("file_path").to_pylist()}
    refs |= {_local(p) for p in table.inspect.all_manifests().column("path").to_pylist()}
    refs |= {_local(s.manifest_list) for s in table.snapshots() if s.manifest_list}
    refs |= {_local(e.metadata_file) for e in table.metadata.metadata_log}
    refs.add(_local(table.metadata_location))
    return refs


def remove_orphans(table: Table, grace: dt.timedelta, dry_run: bool, report: MaintenanceReport) -> None:
    refs = referenced_files(table)
    root = _local(table.location())
    young = dt.datetime.now().timestamp() - grace.total_seconds()
    for p in root.rglob("*"):
        if not p.is_file() or p.resolve() in refs:
            continue
        if p.stat().st_mtime > young:
            report.young_unreferenced += 1        # may belong to a commit still in flight
            continue
        report.orphans.append(str(p))
        report.orphan_bytes += p.stat().st_size
        if not dry_run:
            os.remove(p)


def maintain(sink, source_table: str, *, retain_hours: float = 24, keep_last: int = 10, compact_min_files: int = 32,
             compact_max_bytes: int = 2 << 30, grace_minutes: float = 60, dry_run: bool = False) -> MaintenanceReport | None:
    table = sink.load(source_table)
    if table is None:
        return None
    report = MaintenanceReport(source_table, dry_run)
    table = compact(table, compact_min_files, compact_max_bytes, dry_run, report)
    table = expire(table, dt.timedelta(hours=retain_hours), keep_last, dry_run, report)
    remove_orphans(table.refresh(), dt.timedelta(minutes=grace_minutes), dry_run, report)
    return report
