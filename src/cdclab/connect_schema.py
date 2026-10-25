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
    array (items: any of the above)  JSON list                         -> list<item>

Everything is decoded exactly: no float round-trips for decimals or timestamps.

Postgres `infinity` / `-infinity` in timestamp, timestamptz and date columns become the largest / smallest value
Python can hold (datetime.max / datetime.min, date.max / date.min); reconcile loads the source the same way. Debezium
encodes them as sentinels, and they differ by type (observed on Debezium 3.7.0.Final, not all as documented):

    MicroTimestamp  Long.MAX_VALUE / Long.MIN_VALUE
    Timestamp       9223372036825200000 / -9223372036832400000 (the documented values)
    ZonedTimestamp  "infinity" / "-infinity"
    Date            2146033979 / -2147472691 days when streaming, but -622191234 / -625821272 in the initial
                    snapshot (+infinity comes out negative). All four are far outside Postgres' date range.

Any other value outside Python's range (Postgres allows years up to 294276) raises instead of being clamped.
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


POS_INF_TS, NEG_INF_TS = dt.datetime.max, dt.datetime.min
POS_INF_DATE, NEG_INF_DATE = dt.date.max, dt.date.min
_INF_TS = {2**63 - 1: POS_INF_TS, -2**63: NEG_INF_TS,
           9223372036825200000: POS_INF_TS, -9223372036832400000: NEG_INF_TS}
_INF_DATE = {2146033979: POS_INF_DATE, -2147472691: NEG_INF_DATE,      # streaming (pgoutput)
             -622191234: POS_INF_DATE, -625821272: NEG_INF_DATE}       # snapshot (JDBC): +infinity is NEGATIVE


def _out_of_range(v, what: str):
    return ValueError(f"{what} {v!r} is outside the range Python can represent (years 1-9999) and is not one of "
                      "Debezium's infinity sentinels")


def _days(v):
    if v is None:
        return None
    if v in _INF_DATE:
        return _INF_DATE[v]
    try:
        return EPOCH_DATE + dt.timedelta(days=v)
    except OverflowError:
        raise _out_of_range(v, "date (days since epoch)") from None


def _epoch_offset(unit: str):
    def decode(v):
        if v is None:
            return None
        if v in _INF_TS:
            return _INF_TS[v]
        try:
            return EPOCH + dt.timedelta(**{unit: v})
        except OverflowError:
            raise _out_of_range(v, f"timestamp ({unit} since epoch)") from None
    return decode


_micros = _epoch_offset("microseconds")
_millis = _epoch_offset("milliseconds")


def _nanos(v):
    if v is None:
        return None
    return _INF_TS[v] if v in _INF_TS else _micros(v // 1000)


def _zoned(v):
    if v is None:
        return None
    if v == "infinity":
        return POS_INF_TS.replace(tzinfo=dt.timezone.utc)
    if v == "-infinity":
        return NEG_INF_TS.replace(tzinfo=dt.timezone.utc)
    return dt.datetime.fromisoformat(v.replace("Z", "+00:00")).astimezone(dt.timezone.utc)


def _micro_time(v):
    if v is None:
        return None
    return (dt.datetime.min + dt.timedelta(microseconds=v)).time()


def _list_decoder(item: Callable[[Any], Any]):
    def decode(v):
        return None if v is None else [None if x is None else item(x) for x in v]
    return decode


_PHYSICAL = {
    "int8": pa.int8(), "int16": pa.int16(), "int32": pa.int32(), "int64": pa.int64(),
    # JsonConverter writes FLOAT32 / FLOAT64 as "float" / "double" (checked on the live stack)
    "float": pa.float32(), "double": pa.float64(), "float32": pa.float32(), "float64": pa.float64(),
    "boolean": pa.bool_(), "string": pa.string(), "bytes": pa.binary(),
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
    if typ == "array" and isinstance(field.get("items"), dict):
        item = _column({**field["items"], "field": f"{name}.element"})
        if isinstance(item.arrow, pa.ListType):
            raise ValueError(f"column {name!r}: nested arrays are not supported")
        return Column(name, pa.list_(item.arrow), _list_decoder(item.decode), optional)
    raise ValueError(f"column {name!r}: unsupported Connect type {typ!r} ({logical})")


def row_columns(envelope_schema: dict) -> list[Column]:
    """Columns of the row struct (the `after`/`before` field) inside a Debezium envelope schema."""
    for f in envelope_schema.get("fields", []):
        if f.get("field") in ("after", "before") and f.get("type") == "struct":
            return [column(c) for c in f["fields"]]
    raise ValueError("envelope schema has no before/after struct")


def arrow_schema(columns: list[Column]) -> pa.Schema:
    return pa.schema([pa.field(c.name, c.arrow, nullable=True) for c in columns])
