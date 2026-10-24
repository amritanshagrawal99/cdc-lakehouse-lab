"""Kafka Connect JSON schemas (as emitted by Debezium with schemas.enable=true) -> Arrow types and value decoders.

Debezium encodes logical types on top of a few physical ones; the field's `name` says which:

    io.debezium.time.Date            int32  days since epoch          -> date32
    io.debezium.time.MicroTimestamp  int64  micros since epoch (naive) -> timestamp[us]
    io.debezium.time.Timestamp       int64  millis since epoch (naive) -> timestamp[us]
    io.debezium.time.NanoTimestamp   int64  nanos since epoch (naive)  -> timestamp[us] (truncated)
    io.debezium.time.ZonedTimestamp  string ISO-8601 with offset       -> timestamp[us, UTC]
    io.debezium.time.MicroTime       int64  micros since midnight      -> time64[us]
    org.apache.kafka.connect.data.Decimal  bytes base64, big-endian two's complement, scale in parameters -> decimal128
    io.debezium.data.Json / Uuid / Enum / Xml  string                  -> string

Everything is decoded exactly: no float round-trips for decimals or timestamps.
"""
from __future__ import annotations

import base64
import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pyarrow as pa

EPOCH = dt.datetime(1970, 1, 1)
_TEMPORAL = {"io.debezium.time.Date", "io.debezium.time.MicroTimestamp", "io.debezium.time.Timestamp",
             "io.debezium.time.NanoTimestamp", "io.debezium.time.ZonedTimestamp", "io.debezium.time.MicroTime",
             "io.debezium.time.Time", "io.debezium.time.NanoTime"}
EPOCH_DATE = dt.date(1970, 1, 1)
DECIMAL_NAME = "org.apache.kafka.connect.data.Decimal"
VARIABLE_DECIMAL = "io.debezium.data.VariableScaleDecimal"


@dataclass(frozen=True)
class Column:
    name: str
    arrow: pa.DataType
    decode: Callable[[Any], Any]
    optional: bool
    default: Any = None  # the column's database default (Debezium copies it into the Connect schema), decoded


def _ident(v):
    return v


def _decimal_decoder(scale: int):
    def decode(v):
        if v is None:
            return None
        raw = base64.b64decode(v)
        unscaled = int.from_bytes(raw, "big", signed=True)
        return Decimal(unscaled).scaleb(-scale)
    return decode


def _days(v):
    return None if v is None else EPOCH_DATE + dt.timedelta(days=v)


def _micros(v):
    return None if v is None else EPOCH + dt.timedelta(microseconds=v)


def _millis(v):
    return None if v is None else EPOCH + dt.timedelta(milliseconds=v)


def _nanos(v):
    return None if v is None else EPOCH + dt.timedelta(microseconds=v // 1000)


def _zoned(v):
    if v is None:
        return None
    return dt.datetime.fromisoformat(v.replace("Z", "+00:00")).astimezone(dt.timezone.utc)


def _micro_time(v):
    if v is None:
        return None
    return (dt.datetime.min + dt.timedelta(microseconds=v)).time()


_PHYSICAL = {
    "int8": pa.int8(), "int16": pa.int16(), "int32": pa.int32(), "int64": pa.int64(),
    "float32": pa.float32(), "float64": pa.float64(), "boolean": pa.bool_(), "string": pa.string(), "bytes": pa.binary(),
}


def column(field: dict) -> Column:
    """Arrow type, decoder and default for one Connect schema field."""
    c = _column(field)
    raw_default = field.get("default")
    if raw_default is None:
        return c
    if field.get("name") in _TEMPORAL and raw_default in (0, "1970-01-01T00:00:00Z", "1970-01-01T00:00:00.000000Z"):
        # Debezium exports FUNCTION defaults (now(), current_date) as the epoch: the real value is unknowable here.
        return c
    try:
        default = c.decode(raw_default)
    except Exception:  # noqa: BLE001 - an undecodable default is treated as "no default"
        default = None
    return Column(c.name, c.arrow, c.decode, c.optional, default)


def _column(field: dict) -> Column:
    name, typ, logical = field["field"], field["type"], field.get("name")
    optional = bool(field.get("optional", True))
    params = field.get("parameters") or {}
    if logical == DECIMAL_NAME:
        scale = int(params.get("scale", 0))
        precision = int(params.get("connect.decimal.precision", 38))
        return Column(name, pa.decimal128(min(max(precision, scale, 1), 38), scale), _decimal_decoder(scale), optional)
    if logical == VARIABLE_DECIMAL:
        raise ValueError(f"column {name!r}: VariableScaleDecimal (NUMERIC without scale) is not supported; "
                         "declare NUMERIC(p, s) in Postgres or use decimal.handling.mode=string")
    if logical == "io.debezium.time.Date":
        return Column(name, pa.date32(), _days, optional)
    if logical == "io.debezium.time.MicroTimestamp":
        return Column(name, pa.timestamp("us"), _micros, optional)
    if logical == "io.debezium.time.Timestamp":
        return Column(name, pa.timestamp("us"), _millis, optional)
    if logical == "io.debezium.time.NanoTimestamp":
        return Column(name, pa.timestamp("us"), _nanos, optional)
    if logical == "io.debezium.time.ZonedTimestamp":
        return Column(name, pa.timestamp("us", tz="UTC"), _zoned, optional)
    if logical == "io.debezium.time.Time":          # int32 milliseconds past midnight (time(0-3) under adaptive)
        return Column(name, pa.time64("us"), lambda v: _micro_time(None if v is None else v * 1000), optional)
    if logical == "io.debezium.time.NanoTime":      # int64 nanoseconds past midnight
        return Column(name, pa.time64("us"), lambda v: _micro_time(None if v is None else v // 1000), optional)
    if logical == "io.debezium.time.MicroTime":
        return Column(name, pa.time64("us"), _micro_time, optional)
    if typ in _PHYSICAL:
        decode = (lambda v: None if v is None else base64.b64decode(v)) if typ == "bytes" else _ident
        return Column(name, _PHYSICAL[typ], decode, optional)
    raise ValueError(f"column {name!r}: unsupported Connect type {typ!r} ({logical})")


def row_columns(envelope_schema: dict) -> list[Column]:
    """Columns of the row struct (the `after`/`before` field) inside a Debezium envelope schema."""
    for f in envelope_schema.get("fields", []):
        if f.get("field") in ("after", "before") and f.get("type") == "struct":
            return [column(c) for c in f["fields"]]
    raise ValueError("envelope schema has no before/after struct")


def arrow_schema(columns: list[Column]) -> pa.Schema:
    return pa.schema([pa.field(c.name, c.arrow, nullable=True) for c in columns])
