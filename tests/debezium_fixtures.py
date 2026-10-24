"""Build Kafka records exactly as Debezium's JsonConverter (schemas.enable=true) emits them, for unit tests."""
from __future__ import annotations

import base64
import json
from decimal import Decimal

ROW_FIELDS = [
    {"type": "int32", "optional": False, "field": "id"},
    {"type": "string", "optional": True, "field": "name"},
    {"type": "bytes", "optional": True, "name": "org.apache.kafka.connect.data.Decimal", "version": 1,
     "parameters": {"scale": "2", "connect.decimal.precision": "12"}, "field": "balance"},
    {"type": "int64", "optional": True, "name": "io.debezium.time.MicroTimestamp", "version": 1, "field": "updated_at"},
    {"type": "string", "optional": True, "field": "notes"},
]


def dec(value: str, scale: int = 2) -> str:
    unscaled = int(Decimal(value).scaleb(scale))
    length = max(1, (unscaled.bit_length() + 8) // 8)
    return base64.b64encode(unscaled.to_bytes(length, "big", signed=True)).decode()


def envelope_schema(row_fields=ROW_FIELDS, table="accounts"):
    row = {"type": "struct", "fields": row_fields, "optional": True, "name": f"lab.public.{table}.Value"}
    return {"type": "struct", "name": f"lab.public.{table}.Envelope", "optional": False, "fields": [
        {**row, "field": "before"}, {**row, "field": "after"},
        {"type": "struct", "field": "source", "optional": False, "fields": [
            {"type": "string", "field": "schema"}, {"type": "string", "field": "table"},
            {"type": "int64", "field": "lsn", "optional": True}, {"type": "int64", "field": "txId", "optional": True}]},
        {"type": "string", "field": "op", "optional": False},
        {"type": "int64", "field": "ts_ms", "optional": True},
    ]}


def record(op, after=None, before=None, key=None, table="accounts", lsn=100, tx=1, row_fields=ROW_FIELDS,
           partition=0, offset=0, topic=None, old_key=None, seq=None):
    """(key_bytes, value_bytes, topic, partition, offset, headers) for one change event."""
    key = key if key is not None else {"id": (after or before)["id"]}
    key_schema = {"type": "struct", "optional": False, "name": f"lab.public.{table}.Key",
                  "fields": [{"type": "int32", "optional": False, "field": k} for k in key]}
    k = json.dumps({"schema": key_schema, "payload": key}).encode()
    payload = {"before": before, "after": after, "op": op, "ts_ms": 1_700_000_000_000,
               "source": {"schema": "public", "table": table, "lsn": lsn, "txId": tx,
                          "sequence": json.dumps([str(seq[0]), str(seq[1])]) if seq else None}}
    v = json.dumps({"schema": envelope_schema(row_fields, table), "payload": payload}).encode()
    headers = [("__debezium.oldkey", json.dumps({"schema": key_schema, "payload": old_key}).encode())] if old_key else None
    return k, v, topic or f"lab.public.{table}", partition, offset, headers


def truncate(table="accounts", lsn=500, partition=0, offset=0):
    payload = {"before": None, "after": None, "op": "t", "ts_ms": 1, "source": {"schema": "public", "table": table, "lsn": lsn}}
    return None, json.dumps({"schema": envelope_schema(table=table), "payload": payload}).encode(), f"lab.public.{table}", partition, offset


def row(id, name="a", balance="10.00", updated_at=1_700_000_000_000_000, notes="n"):
    return {"id": id, "name": name, "balance": dec(balance) if balance is not None else None, "updated_at": updated_at,
            "notes": notes}
