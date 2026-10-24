"""Debezium change events, parsed from Kafka messages (JsonConverter, schemas.enable=true)."""
from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field

from .connect_schema import Column, row_columns

# Debezium replaces a TOASTed column that the UPDATE did not touch with a placeholder (Postgres does not send it).
# The shape depends on the column type: text / jsonb get the string, bytea its bytes (base64 in JSON), arrays a list.
TOAST_PLACEHOLDER = "__debezium_unavailable_value"
_PLACEHOLDER_FORMS = {TOAST_PLACEHOLDER, base64.b64encode(TOAST_PLACEHOLDER.encode()).decode()}


def is_placeholder(v) -> bool:
    if isinstance(v, str):
        return v in _PLACEHOLDER_FORMS
    if isinstance(v, (bytes, bytearray)):
        return bytes(v) == TOAST_PLACEHOLDER.encode()
    if isinstance(v, list):
        return bool(v) and all(is_placeholder(x) for x in v)
    return False


@dataclass
class ChangeEvent:
    table: str                     # schema.table
    op: str                        # c(reate) u(pdate) d(elete) r(ead, snapshot) t(runcate)
    key: tuple                     # primary-key values, in key-schema order
    key_names: tuple[str, ...]
    before: dict | None
    after: dict | None
    columns: list[Column] = field(default_factory=list)  # row columns from the event's own schema
    lsn: int | None = None
    tx_id: int | None = None
    ts_ms: int | None = None
    topic: str = ""
    partition: int = 0
    offset: int = 0
    old_key: tuple | None = None   # set on the create half of a primary-key change (header __debezium.oldkey)
    position: tuple[int, int] | None = None   # (lastCommitLsn, lsn) from source.sequence; None for snapshot reads

    @property
    def unavailable(self) -> set[str]:
        """Columns whose value Postgres did not send (unchanged TOAST): keep the stored value."""
        return {k for k, v in (self.after or {}).items() if v == TOAST_PLACEHOLDER}


def _decode_row(raw: dict | None, columns: list[Column]) -> dict | None:
    if raw is None:
        return None
    by_name = {c.name: c for c in columns}
    out = {}
    for k, v in raw.items():
        col = by_name.get(k)
        if is_placeholder(v):
            out[k] = TOAST_PLACEHOLDER      # one sentinel whatever the column type
        else:
            out[k] = v if col is None else col.decode(v)
    return out


def _key_values(raw: bytes | None, columns: list[Column]) -> tuple[tuple, tuple[str, ...]]:
    k = json.loads(raw) if raw else {}
    k_payload = k.get("payload", k) if isinstance(k, dict) else {}
    names = tuple(f["field"] for f in (k.get("schema") or {}).get("fields", [])) or tuple(k_payload)
    by_name = {c.name: c for c in columns}
    return tuple(by_name[n].decode(k_payload[n]) if n in by_name else k_payload[n] for n in names), names


def _header(headers, name: str) -> bytes | None:
    for k, v in headers or ():
        if k == name:
            return v
    return None


def has_header(headers, name: str) -> bool:
    return any(k == name for k, _ in headers or ())


def parse(key: bytes | None, value: bytes | None, topic: str = "", partition: int = 0, offset: int = 0,
          headers=None) -> ChangeEvent | None:
    """One Kafka record -> ChangeEvent. Returns None for tombstones (null value after a delete): the delete
    event that precedes every tombstone already carries the change; tombstones only exist for log compaction.

    `headers` are the Kafka record headers; Debezium marks the create half of a primary-key change with
    __debezium.oldkey, which the compactor needs to carry unchanged TOAST values over from the old key."""
    if value is None:
        return None
    msg = json.loads(value)
    schema, payload = msg.get("schema") or {}, msg.get("payload")
    if payload is None:
        return None
    op = payload["op"]
    source = payload.get("source") or {}
    table = f"{source.get('schema', 'public')}.{source.get('table', '')}"
    if op == "t":  # truncate: no row, no key
        return ChangeEvent(table, "t", (), (), None, None, [], source.get("lsn"), source.get("txId"), payload.get("ts_ms"),
                           topic, partition, offset, None, position(source, op))
    columns = row_columns(schema)
    key_vals, key_names = _key_values(key, columns)
    old_raw = _header(headers, "__debezium.oldkey")
    old_key = _key_values(old_raw, columns)[0] if old_raw else None
    return ChangeEvent(table, op, key_vals, key_names, _decode_row(payload.get("before"), columns),
                       _decode_row(payload.get("after"), columns), columns, source.get("lsn"), source.get("txId"),
                       payload.get("ts_ms"), topic, partition, offset, old_key, position(source, op))


def position(source: dict, op: str) -> tuple[int, int] | None:
    """Debezium's source.sequence is a JSON string ["lastCommitLsn", "lsn"]. Compared as a pair it never decreases
    in emission order (raw lsn alone does not: a transaction that commits later can hold lower LSNs). Snapshot reads
    and events without a commit LSN have no comparable position."""
    if op == "r":
        return None
    raw = source.get("sequence")
    try:
        seq = json.loads(raw) if isinstance(raw, str) else raw
        return (int(seq[0]), int(seq[1])) if seq and seq[0] is not None and seq[1] is not None else None
    except (ValueError, TypeError, IndexError):
        return None


def tx_id(value: bytes | None) -> int | None:
    """The source transaction id of a record, cheaply (tombstones have none)."""
    if value is None:
        return None
    payload = json.loads(value).get("payload") or {}
    return (payload.get("source") or {}).get("txId")
