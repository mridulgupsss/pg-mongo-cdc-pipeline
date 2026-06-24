"""Pure unit tests for the transform layer — no database required.
Run with: pytest tests/test_transform.py   (or python -m pytest)
"""
from bson.decimal128 import Decimal128

from pipeline.transform import (
    MongoOp, build_document, build_pipeline_update, canonical_timestamp,
    change_to_op, coalesce_ops, coerce_value, diff_changed,
)

TRACKED = {"order_id", "customer_email", "total_amount", "status", "is_paid",
           "created_at", "updated_at"}


def test_coerce_numeric_is_exact_decimal():
    assert coerce_value("numeric", "19.99") == Decimal128("19.99")


def test_coerce_bool_from_wal_string_and_native():
    assert coerce_value("boolean", "t") is True
    assert coerce_value("boolean", False) is False


def test_coerce_integer_and_null():
    assert coerce_value("bigint", "42") == 42
    assert coerce_value("text", None) is None


def test_canonical_timestamp_normalises_space_and_offset():
    a = canonical_timestamp("2024-06-23 12:00:00+00")
    b = canonical_timestamp("2024-06-23T12:00:00+00:00")
    assert a == b


def test_diff_changed_only_returns_changed_columns():
    new = {"status": "paid", "total_amount": "10.00"}
    old = {"status": "pending", "total_amount": "10.00"}
    assert diff_changed(new, old) == {"status": "paid"}


def _insert_change():
    return {
        "action": "I", "table": "orders",
        "columns": [
            {"name": "order_id", "type": "bigint", "value": 1},
            {"name": "total_amount", "type": "numeric", "value": "10.00"},
            {"name": "status", "type": "order_status", "value": "pending"},
            {"name": "secret_col", "type": "text", "value": "x"},   # untracked -> dropped
        ],
    }


def test_change_to_op_insert_drops_untracked_and_stamps_lsn():
    op = change_to_op(_insert_change(), "order_id", TRACKED, lsn=500)
    assert op.pk_value == 1 and op.lsn == 500
    assert "secret_col" not in op.set_fields
    assert op.set_fields["total_amount"] == Decimal128("10.00")
    assert op.set_fields["_lsn"] == 500


def test_change_to_op_update_is_partial():
    change = {
        "action": "U", "table": "orders",
        "columns": [{"name": "order_id", "type": "bigint", "value": 1},
                    {"name": "status", "type": "order_status", "value": "paid"}],
        "identity": [{"name": "order_id", "type": "bigint", "value": 1},
                     {"name": "status", "type": "order_status", "value": "pending"}],
    }
    op = change_to_op(change, "order_id", TRACKED, lsn=600)
    assert set(op.set_fields) == {"status", "_lsn"}     # only the changed column + lsn


def test_change_to_op_delete_is_soft():
    change = {"action": "D", "table": "orders", "timestamp": "2024-06-23 12:00:00+00",
              "identity": [{"name": "order_id", "type": "bigint", "value": 9}]}
    op = change_to_op(change, "order_id", TRACKED, lsn=700)
    assert op.is_delete and op.pk_value == 9
    assert op.set_fields["deleted_at"] is not None


def test_change_to_op_ignores_non_dml():
    assert change_to_op({"action": "B"}, "order_id", TRACKED, 1) is None


def test_pipeline_update_is_lsn_guarded():
    update = build_pipeline_update({"status": "paid", "_lsn": 5}, lsn=5)
    cond = update[0]["$set"]["status"]["$cond"]
    assert cond[0] == {"$gt": [5, {"$ifNull": ["$_lsn", -1]}]}


def test_coalesce_distinct_keys_are_untouched():
    ops = [MongoOp("orders", 1, {"status": "paid", "_lsn": 1}, lsn=1),
           MongoOp("orders", 2, {"status": "shipped", "_lsn": 2}, lsn=2)]
    out = coalesce_ops(ops)
    assert len(out) == 2
    assert {o.pk_value for o in out} == {1, 2}


def test_coalesce_merges_disjoint_fields_of_same_row():
    # The bug this fixes: two partial updates to the same row on different fields.
    # A row-level guard under an unordered bulk write could drop the older field;
    # after coalescing there is a single op carrying BOTH fields.
    ops = [MongoOp("orders", 5, {"status": "paid", "_lsn": 100}, lsn=100),
           MongoOp("orders", 5, {"total_amount": 999, "_lsn": 200}, lsn=200)]
    out = coalesce_ops(ops)
    assert len(out) == 1
    merged = out[0]
    assert merged.set_fields["status"] == "paid"        # older field preserved
    assert merged.set_fields["total_amount"] == 999     # newer field present
    assert merged.set_fields["_lsn"] == 200 and merged.lsn == 200


def test_coalesce_same_field_keeps_highest_lsn_regardless_of_order():
    # Order-independent: the newer value wins even if the older op appears last.
    ops = [MongoOp("orders", 5, {"status": "shipped", "_lsn": 200}, lsn=200),
           MongoOp("orders", 5, {"status": "paid", "_lsn": 100}, lsn=100)]
    out = coalesce_ops(ops)
    assert len(out) == 1
    assert out[0].set_fields["status"] == "shipped" and out[0].lsn == 200


def test_coalesce_insert_then_update_yields_complete_doc():
    insert = MongoOp("orders", 5,
                     {"customer_email": "a@x.com", "status": "pending", "_lsn": 100}, lsn=100)
    update = MongoOp("orders", 5, {"status": "paid", "_lsn": 200}, lsn=200)
    out = coalesce_ops([insert, update])
    assert len(out) == 1
    fields = out[0].set_fields
    assert fields["customer_email"] == "a@x.com"   # insert field not lost
    assert fields["status"] == "paid"              # newer status wins
    assert fields["_lsn"] == 200


def test_coalesce_does_not_merge_across_tables_with_same_pk():
    ops = [MongoOp("orders", 5, {"_lsn": 1}, lsn=1),
           MongoOp("order_items", 5, {"_lsn": 2}, lsn=2)]
    out = coalesce_ops(ops)
    assert len(out) == 2


def test_build_document_matches_insert_shape():
    doc = build_document(
        {"order_id": 1, "total_amount": "10.00"},
        {"order_id": "bigint", "total_amount": "numeric"},
        "order_id", {"order_id", "total_amount"}, lsn=10)
    assert doc["_id"] == 1 and doc["deleted_at"] is None
    assert doc["total_amount"] == Decimal128("10.00")
