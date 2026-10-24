import datetime as dt
from decimal import Decimal

import pyarrow as pa
import pytest
from debezium_fixtures import record, row, truncate

from cdclab.compact import compact
from cdclab.connect_schema import arrow_schema, column
from cdclab.events import TOAST_PLACEHOLDER, parse


def ev(*args, **kw):
    return parse(*record(*args, **kw))


def test_decodes_logical_types_exactly():
    e = ev("c", after=row(1, balance="-1234.56", updated_at=1_700_000_000_123_456))
    assert e.table == "public.accounts" and e.op == "c" and e.key == (1,) and e.key_names == ("id",)
    assert e.after["balance"] == Decimal("-1234.56")
    assert e.after["updated_at"] == dt.datetime(2023, 11, 14, 22, 13, 20, 123456)
    assert arrow_schema(e.columns).field("balance").type == pa.decimal128(12, 2)


@pytest.mark.parametrize("field,raw,expected", [
    ({"field": "d", "type": "int32", "name": "io.debezium.time.Date"}, 19_723, dt.date(2024, 1, 1)),
    ({"field": "z", "type": "string", "name": "io.debezium.time.ZonedTimestamp"}, "2024-01-01T05:30:00+05:30",
     dt.datetime(2024, 1, 1, 0, 0, tzinfo=dt.timezone.utc)),
    ({"field": "t", "type": "int64", "name": "io.debezium.time.MicroTime"}, 3_600_000_001, dt.time(1, 0, 0, 1)),
    ({"field": "m", "type": "int64", "name": "io.debezium.time.Timestamp"}, 1_000, dt.datetime(1970, 1, 1, 0, 0, 1)),
    ({"field": "b", "type": "boolean"}, True, True),
])
def test_column_decoders(field, raw, expected):
    assert column(field).decode(raw) == expected


def test_variable_scale_decimal_is_rejected_with_advice():
    with pytest.raises(ValueError, match="NUMERIC"):
        column({"field": "x", "type": "struct", "name": "io.debezium.data.VariableScaleDecimal"})


def test_tombstone_is_ignored():
    assert parse(b'{"id": 1}', None) is None


def test_compaction_last_state_wins():
    plan = compact([ev("c", after=row(1, "a")), ev("u", after=row(1, "b"), before=row(1, "a")),
                    ev("c", after=row(2)), ev("d", before=row(2)),            # insert then delete -> delete
                    ev("d", before=row(3)), ev("c", after=row(3, "again"))])  # delete then insert -> upsert
    p = plan["public.accounts"]
    assert p.upserts[(1,)]["name"] == "b" and p.upserts[(3,)]["name"] == "again"
    assert p.deletes == {(2,)} and p.events == 6 and not p.truncate


def test_truncate_discards_earlier_events():
    plan = compact([ev("c", after=row(1)), parse(*truncate()), ev("c", after=row(2))])["public.accounts"]
    assert plan.truncate and list(plan.upserts) == [(2,)] and not plan.deletes


def test_toast_placeholder_filled_from_batch_or_flagged():
    toast = row(1, notes=TOAST_PLACEHOLDER)
    p = compact([ev("c", after=row(1, notes="big text")), ev("u", after=toast)])["public.accounts"]
    assert p.upserts[(1,)]["notes"] == "big text" and not p.fill_from_table
    p = compact([ev("u", after=toast)])["public.accounts"]          # not in this batch: read from the lake
    assert p.fill_from_table == {(1,): {"notes"}} and p.upserts[(1,)]["notes"] is None
    p = compact([ev("u", after=toast), ev("u", after=row(1, notes="new"))])["public.accounts"]
    assert not p.fill_from_table and p.upserts[(1,)]["notes"] == "new"   # a later full value clears the gap


def test_primary_key_change_is_delete_plus_create():
    # Debezium emits a delete under the old key and a create under the new key.
    p = compact([ev("d", before=row(1)), ev("c", after=row(10))])["public.accounts"]
    assert p.deletes == {(1,)} and list(p.upserts) == [(10,)]


def test_function_defaults_are_not_trusted():
    """Debezium exports now() / current_date defaults as the epoch; backfilling them would be wrong."""
    assert column({"field": "c", "type": "int64", "name": "io.debezium.time.MicroTimestamp", "default": 0}).default is None
    assert column({"field": "d", "type": "int32", "name": "io.debezium.time.Date", "default": 0}).default is None
    assert column({"field": "s", "type": "string", "default": "standard"}).default == "standard"


def test_table_without_primary_key_fails_loudly():
    from cdclab.compact import compact
    k, v, *rest = record("c", after=row(1), key={})
    with pytest.raises(ValueError, match="no primary key"):
        compact([parse(None, v)])
