"""Kafka -> Iceberg consumer with exactly-once effect.

Positions live in Iceberg, not in the consumer group: on assignment each partition is seeked to (offset stored in the
table) + 1, and any record at or before a stored offset is skipped. Kafka offsets are still committed after each batch,
but only so lag shows up in monitoring; correctness never depends on them.
"""
from __future__ import annotations

import logging
import os
from collections import defaultdict
from collections.abc import Iterable

from pyiceberg.exceptions import CommitFailedException, TableAlreadyExistsError, ValidationException

from .compact import compact
from .events import has_header, parse, tx_id
from .sink import OFFSET_PREFIX, POS_PREFIX, ApplyResult, IcebergSink

log = logging.getLogger("cdclab")
Record = tuple  # (key: bytes | None, value: bytes | None, topic: str, partition: int, offset: int[, headers])
NEWKEY_HEADER = "__debezium.newkey"   # on the delete half of a primary-key change: its create half follows


def topic_table(topic: str, prefix: str) -> str:
    """Debezium topic `<prefix>.<schema>.<table>` -> source table `<schema>.<table>`."""
    if not topic.startswith(prefix + "."):
        raise ValueError(f"topic {topic!r} does not start with prefix {prefix!r}")
    return topic[len(prefix) + 1:]


def stored_state(properties: dict[str, str]) -> tuple[dict[tuple[str, int], int], dict[tuple[str, int], tuple[int, int]]]:
    """(last applied offset, high-water Debezium position) per topic-partition, from a table's properties."""
    offsets, high = {}, {}
    for k, v in properties.items():
        if k.startswith(OFFSET_PREFIX):
            topic, _, part = k[len(OFFSET_PREFIX):].rpartition(".")
            offsets[(topic, int(part))] = int(v)
        elif k.startswith(POS_PREFIX):
            topic, _, part = k[len(POS_PREFIX):].rpartition(".")
            a, b = v.split(",")
            high[(topic, int(part))] = (int(a), int(b))
    return offsets, high


class Applier:
    """Turns Kafka records into exactly-once Iceberg commits. Two filters, both stored atomically with the data:

    * Kafka offset: records at or before the stored offset are already in the lake (consumer crash and replay).
    * Debezium position: after a Kafka Connect crash Debezium re-emits events at NEW offsets (it is at-least-once).
      Their (lastCommitLsn, lsn) position is at or below the stored high-water mark, so they are dropped. Without
      this, a replayed update whose TOAST placeholder is filled from current lake state can leave a stale value.

    The applier keeps no state of its own. Every attempt loads the table, reads the offsets and positions from THAT
    snapshot, and commits against it: if anyone else committed in between, pyiceberg rejects the commit and the
    attempt starts over. A stale in-memory copy of the offsets could otherwise apply a batch twice.
    """

    def __init__(self, sink: IcebergSink, prefix: str):
        self.sink, self.prefix = sink, prefix
        self.commits = 0
        self.skipped_replays = 0

    def position(self, topic: str, partition: int) -> int | None:
        """Last applied offset for a partition, read from the table (used to seek on assignment)."""
        return self.sink.stored_offsets(topic_table(topic, self.prefix)).get((topic, partition))

    def apply(self, records: Iterable[Record]) -> list[ApplyResult]:
        records = list(records)
        by_table: dict[str, list[Record]] = defaultdict(list)
        for r in records:
            by_table[topic_table(r[2], self.prefix)].append(r)
        results = []
        for table, recs in by_table.items():
            res = self._apply_table(table, recs)
            if res is not None:
                results.append(res)
                self.commits += 1
                crash = os.environ.get("CDCLAB_CRASH_AFTER_COMMITS")
                if crash and self.commits >= int(crash):
                    # Chaos hook for tests: die after the Iceberg commit, before the Kafka offset commit.
                    log.warning("chaos: exiting after %s Iceberg commits", self.commits)
                    os._exit(17)
        return results

    def _apply_table(self, table: str, recs: list[Record], attempts: int = 6) -> ApplyResult | None:
        schema_changed = backfilled = 0
        for attempt in range(attempts):
            t = self.sink.load(table)
            stored_off, stored_high = stored_state(t.properties) if t is not None else ({}, {})
            fresh = sorted((r for r in recs if r[4] > stored_off.get((r[2], r[3]), -1)), key=lambda r: (r[3], r[4]))
            if not fresh:
                return None  # everything here is already in the lake (replay after a crash, or another writer)
            offsets: dict[tuple[str, int], int] = {}
            high = dict(stored_high)
            events, skipped = [], 0
            for key, value, topic, partition, offset, *rest in fresh:
                tp = (topic, partition)
                offsets[tp] = max(offset, offsets.get(tp, -1))   # advance over EVERY record, applied or not
                ev = parse(key, value, topic, partition, offset, rest[0] if rest else None)
                if ev is None:
                    continue
                if ev.position is not None:
                    # Compared with the position stored BEFORE this batch: a replayed event is at or below it.
                    # Ties within the batch (both halves of a primary-key change) are above it and pass.
                    if tp in stored_high and ev.position <= stored_high[tp]:
                        skipped += 1
                        continue
                    high[tp] = max(ev.position, high.get(tp, ev.position))
                events.append(ev)
            pos_props = {f"{POS_PREFIX}{tp}.{p}": f"{a},{b}" for (tp, p), (a, b) in high.items()}
            plan = compact(events).get(table) if events else None
            try:
                if plan is not None and plan.columns and t is not None and self.sink.needs_evolution(t, plan):
                    schema_changed, backfilled = 1, self.sink.evolve(t, plan)
                    continue   # the schema commit is separate: reload, re-read offsets, then apply the data
                if plan is not None and (plan.columns or (plan.truncate and t is not None)):
                    result = self.sink.apply(plan, offsets, pos_props, table=t)
                elif t is not None:
                    self.sink.record_state(table, offsets, pos_props, table=t)   # tombstones / replays: advance anyway
                    result = ApplyResult(table, 0, 0, False, False)
                else:
                    return None   # nothing applicable for a table that does not exist yet
            except (CommitFailedException, ValidationException, TableAlreadyExistsError) as e:
                # Someone else committed first. The next attempt reloads the table and recomputes from scratch.
                log.warning("%s: commit conflict (%s), retrying (%s/%s)", table, type(e).__name__, attempt + 1, attempts)
                continue
            self.skipped_replays += skipped
            if skipped:
                log.info("%s: skipped %s replayed events at or below the stored Debezium position", table, skipped)
            result.schema_changed = bool(schema_changed) or result.schema_changed
            result.backfilled += backfilled
            return result
        raise RuntimeError(f"{table}: gave up after {attempts} conflicting commits")


def split_trailing_transaction(records: list[Record], full: bool = False,
                               max_hold: int = 10_000) -> tuple[list[Record], list[Record]]:
    """(records to apply now, trailing records of the last source transaction to hold back).

    Debezium emits a transaction's events together, but a poll can still end in the middle of one. Holding the
    tail back until more arrives keeps each source transaction in one commit and, above all, keeps the two halves
    of a primary-key change (delete old key, create new key) in one batch. A batch that is entirely one transaction
    is held unless it filled the poll (then it is applied rather than stalled).
    """
    if not records:
        return [], []
    i = len(records) - 1
    while i >= 0 and records[i][1] is None:   # tombstones belong to the transaction before them
        i -= 1
    if i < 0:
        return records, []
    last_tx = tx_id(records[i][1])
    if last_tx is None:
        return records, []
    j = i
    while j > 0 and (records[j - 1][1] is None or tx_id(records[j - 1][1]) == last_tx):
        j -= 1
    if (full and j == 0) or len(records) - j > max_hold:
        return records, []
    return records[:j], records[j:]


def applied_offsets(records: list[Record]) -> dict[tuple[str, int], int]:
    """Next offset to consume per topic-partition once `records` are applied (what gets committed to Kafka)."""
    out: dict[tuple[str, int], int] = {}
    for r in records:
        out[(r[2], r[3])] = max(out.get((r[2], r[3]), 0), r[4] + 1)
    return out


def awaiting_create_half(records: list[Record]) -> bool:
    """True if the last change event is the delete half of a primary-key change whose create half has not arrived.
    Such a delete must never be applied alone: the create may carry TOAST placeholders only the old row can fill."""
    for r in reversed(records):
        if r[1] is not None:
            return has_header(r[5] if len(r) > 5 else None, NEWKEY_HEADER)
    return False


def run(bootstrap: str, prefix: str, topics: list[str], sink: IcebergSink, group: str = "cdclab",
        batch: int = 500, idle_exit_s: float | None = None) -> int:
    """Consume until idle for `idle_exit_s` seconds (None = forever). Returns the number of Iceberg commits."""
    import time

    from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaException, TopicPartition

    applier = Applier(sink, prefix)

    def on_assign(consumer, partitions):  # noqa: ARG001 - signature fixed by confluent-kafka
        for tp in partitions:
            stored = applier.position(tp.topic, tp.partition)
            tp.offset = stored + 1 if stored is not None else OFFSET_BEGINNING
            log.info("assign %s[%s] -> %s", tp.topic, tp.partition, tp.offset)
        consumer.assign(partitions)
        assigned.update((tp.topic, tp.partition) for tp in partitions)

    c = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False,
                  "auto.offset.reset": "error",   # never silently skip data lost to retention
                  "isolation.level": "read_committed",
                  # A crashed consumer never leaves the group; its partitions only move after this timeout.
                  "session.timeout.ms": 10_000, "heartbeat.interval.ms": 2_000})
    c.subscribe(topics, on_assign=on_assign)
    assigned: set[tuple[str, int]] = set()
    last = time.monotonic()
    held: list[Record] = []
    held_since: float | None = None
    try:
        while True:
            msgs = c.consume(num_messages=batch, timeout=1.0)
            for m in msgs:
                if m.error() is not None:
                    raise KafkaException(m.error())   # e.g. auto.offset.reset=error: the stored offset is gone
            new = [(m.key(), m.value(), m.topic(), m.partition(), m.offset(), m.headers()) for m in msgs]
            if not new:
                if held and not awaiting_create_half(held):
                    ready, held = held, []        # nothing more is coming: the held transaction is complete
                else:
                    if held and held_since is not None and time.monotonic() - held_since > 30:
                        log.warning("holding %s records: a primary-key change's create half has not arrived", len(held))
                        held_since = time.monotonic()
                    if not assigned:
                        last = time.monotonic()   # idle time only counts once partitions are ours
                    elif idle_exit_s is not None and time.monotonic() - last > idle_exit_s:
                        break
                    continue
            else:
                last = time.monotonic()
                ready, held = split_trailing_transaction(held + new, full=len(msgs) == batch)
                if held and awaiting_create_half(ready):
                    ready, held = [], ready + held   # never let a PK-change delete through without its create
            held_since = time.monotonic() if held else None
            for r in applier.apply(ready):
                log.info("%s: +%s upserts, -%s deletes%s%s%s", r.table, r.upserted, r.deleted_keys,
                         " (truncated)" if r.truncated else "", " (schema evolved)" if r.schema_changed else "",
                         f" (backfilled {r.backfilled} rows)" if r.backfilled else "")
            if ready:
                # Commit exactly what was applied (held records are not), explicitly: librdkafka's implicit commit
                # raises _NO_OFFSET when a batch was flushed from `held` without consuming anything new.
                c.commit(offsets=[TopicPartition(t, p, o) for (t, p), o in applied_offsets(ready).items()],
                         asynchronous=False)
    finally:
        c.close()
    return applier.commits
