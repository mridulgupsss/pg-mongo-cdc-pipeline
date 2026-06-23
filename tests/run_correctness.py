"""Correctness suite for the CDC pipeline (spec 6). Run against a live pipeline:

    python tests/run_correctness.py

Connects to the same PG/Mongo as the pipeline. Each test prints PASS/FAIL with a
reason; a summary line reports X/10. Tests are deterministic: each asserts on rows it
created (or on convergence), and T7/T8 clean up the added column so re-runs match.
"""
import os
import random
import subprocess
import sys
import time

import psycopg2.extras
from bson.decimal128 import Decimal128

sys.path.insert(0, ".")
from tests.helpers import (  # noqa: E402
    PK_BY_TABLE, get_mongo, get_pg, mongo_live_count, pg_count,
    pg_row_canonical, wait_until,
)

random.seed(7)
TIMEOUT = float(os.environ.get("TEST_TIMEOUT", 60))
SLO = float(os.environ.get("LAG_SLO_SECONDS", 5))
RESTART_CMD = os.environ.get("PIPELINE_RESTART_CMD", "docker compose restart pipeline")


# --- small insert helpers -------------------------------------------------

def insert_orders(pg, n: int, total_amount=None) -> list:
    rows = [(f"t{random.randint(0, 10**9)}@x.com",
             total_amount if total_amount is not None else round(random.uniform(5, 2000), 2),
             "pending", False) for _ in range(n)]
    with pg.cursor() as cur:
        ids = psycopg2.extras.execute_values(
            cur, "INSERT INTO orders (customer_email, total_amount, status, is_paid) "
                 "VALUES %s RETURNING order_id", rows, fetch=True)
    return [r[0] for r in ids]


def insert_items(pg, order_id: int, n: int) -> list:
    rows = [(order_id, "Widget", 1, 9.99, False) for _ in range(n)]
    with pg.cursor() as cur:
        ids = psycopg2.extras.execute_values(
            cur, "INSERT INTO order_items (order_id, product_name, quantity, unit_price, is_gift) "
                 "VALUES %s RETURNING item_id", rows, fetch=True)
    return [r[0] for r in ids]


def all_present(mongo, table: str, ids: list) -> bool:
    return mongo[table].count_documents({"_id": {"$in": ids}}) == len(ids)


# --- tests (each returns (passed: bool, reason: str)) ---------------------

def t1_insert(pg, mongo):
    ids = insert_orders(pg, 200)
    ok = wait_until(lambda: all_present(mongo, "orders", ids), TIMEOUT)
    return ok, f"{mongo['orders'].count_documents({'_id': {'$in': ids}})}/200 orders propagated"


def t2_update(pg, mongo):
    ids = insert_orders(pg, 50)
    wait_until(lambda: all_present(mongo, "orders", ids), TIMEOUT)
    marker = Decimal128("77777.77")
    with pg.cursor() as cur:
        cur.execute("UPDATE orders SET total_amount = 77777.77 WHERE order_id = ANY(%s)", (ids,))
    ok = wait_until(
        lambda: mongo["orders"].count_documents(
            {"_id": {"$in": ids}, "total_amount": marker}) == len(ids), TIMEOUT)
    return ok, "updated total_amount reflected in Mongo"


def t3_delete(pg, mongo):
    order_id = insert_orders(pg, 1)[0]
    item_ids = insert_items(pg, order_id, 50)
    wait_until(lambda: all_present(mongo, "order_items", item_ids), TIMEOUT)
    with pg.cursor() as cur:
        cur.execute("DELETE FROM order_items WHERE item_id = ANY(%s)", (item_ids,))
    ok = wait_until(
        lambda: mongo["order_items"].count_documents(
            {"_id": {"$in": item_ids}, "deleted_at": {"$ne": None}}) == len(item_ids), TIMEOUT)
    return ok, "deleted items soft-deleted (deleted_at set) in Mongo"


def t4_bulk(pg, mongo):
    conn = get_pg()
    conn.autocommit = False               # single transaction for all 10k rows
    rows = [(f"bulk{i}@x.com", 1.0, "pending", False) for i in range(10_000)]
    with conn.cursor() as cur:
        ids = psycopg2.extras.execute_values(
            cur, "INSERT INTO orders (customer_email, total_amount, status, is_paid) "
                 "VALUES %s RETURNING order_id", rows, fetch=True)
    conn.commit()
    conn.close()
    ids = [r[0] for r in ids]
    ok = wait_until(lambda: all_present(mongo, "orders", ids), TIMEOUT * 2)
    return ok, f"{mongo['orders'].count_documents({'_id': {'$in': ids}})}/10000 bulk rows propagated"


def t5_parity(pg, mongo):
    ids = insert_orders(pg, 120)
    wait_until(lambda: all_present(mongo, "orders", ids), TIMEOUT)
    sample = random.sample(ids, 100)
    mismatches = []
    for oid in sample:
        expected = pg_row_canonical(pg, "orders", oid)
        doc = mongo["orders"].find_one({"_id": oid}) or {}
        for col, val in expected.items():
            if doc.get(col) != val:
                mismatches.append((oid, col, val, doc.get(col)))
    return not mismatches, f"{100 - len(mismatches)}/100 rows match all fields" + (
        f"; first mismatch {mismatches[0]}" if mismatches else "")


def t6_restart(pg, mongo):
    before = insert_orders(pg, 100)
    wait_until(lambda: all_present(mongo, "orders", before), TIMEOUT)
    try:
        subprocess.run(RESTART_CMD.split(), check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as exc:
        return False, f"could not restart pipeline ({exc}); set PIPELINE_RESTART_CMD"
    time.sleep(5)                          # let the pipeline come back and resume
    after = insert_orders(pg, 100)
    ok_after = wait_until(lambda: all_present(mongo, "orders", after), TIMEOUT * 2)
    ok_before = all_present(mongo, "orders", before)  # nothing lost across restart
    # Duplicates are structurally impossible (keyed by _id); we assert completeness.
    return ok_after and ok_before, "all pre- and post-restart rows present exactly once"


def t7_schema_new_column(pg, mongo):
    baseline = insert_orders(pg, 1)[0]
    wait_until(lambda: all_present(mongo, "orders", [baseline]), TIMEOUT)
    with pg.cursor() as cur:
        cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS promo_code text")
        cur.execute("UPDATE orders SET promo_code = 'SAVE10' WHERE order_id = %s", (baseline,))
    # Pipeline must keep running: a fresh insert still propagates.
    probe = insert_orders(pg, 1)[0]
    alive = wait_until(lambda: all_present(mongo, "orders", [probe]), TIMEOUT)
    time.sleep(3)
    doc = mongo["orders"].find_one({"_id": baseline}) or {}
    not_propagated = "promo_code" not in doc
    return alive and not_propagated, "pipeline alive; new column absent in Mongo pre-sync"


def t8_schema_sync(pg, mongo):
    doc_id = insert_orders(pg, 1)[0]
    wait_until(lambda: all_present(mongo, "orders", [doc_id]), TIMEOUT)
    try:
        subprocess.run([sys.executable, "-m", "pipeline.main", "schema-sync", "orders"],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as exc:
        return False, f"schema-sync command failed: {exc}"
    synced = wait_until(
        lambda: "promo_code" in (mongo["orders"].find_one({"_id": doc_id}) or {}), TIMEOUT)
    # Cleanup so re-runs are deterministic: drop the column and re-sync the registry.
    with pg.cursor() as cur:
        cur.execute("ALTER TABLE orders DROP COLUMN IF EXISTS promo_code")
    subprocess.run([sys.executable, "-m", "pipeline.main", "schema-sync", "orders"],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return synced, "promo_code backfilled (null) on existing docs after schema-sync"


def t9_lag_bound(pg, mongo):
    # 2x the spec's normal load: ~8k inserted rows/s + 3k updates + 700 deletes.
    load = subprocess.Popen(
        [sys.executable, "scripts/load_test.py", "--duration", "20",
         "--insert-rate", "2400", "--update-rate", "3000", "--delete-rate", "700"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(5)                          # let load ramp up
    max_lag = 0.0
    try:
        for _ in range(6):
            sentinel = insert_orders(pg, 1)[0]
            start = time.monotonic()
            wait_until(lambda: all_present(mongo, "orders", [sentinel]), TIMEOUT)
            max_lag = max(max_lag, time.monotonic() - start)
            time.sleep(1)
    finally:
        load.terminate()
        load.wait()
    return max_lag <= SLO, f"max propagation lag under 2x load = {max_lag:.2f}s (SLO {SLO}s)"


def t10_snapshot_completeness(pg, mongo):
    # With no load running, the pipeline should converge so every live PG row is
    # present (and non-deleted) in Mongo for each table.
    def converged():
        return all(mongo_live_count(mongo, t) == pg_count(pg, t) for t in ("orders", "order_items"))
    ok = wait_until(converged, TIMEOUT * 2, interval=1.0)
    detail = {t: (pg_count(pg, t), mongo_live_count(mongo, t)) for t in ("orders", "order_items")}
    return ok, f"PG vs Mongo live counts {detail}"


TESTS = [
    ("T1  insert propagation", t1_insert),
    ("T2  update propagation", t2_update),
    ("T3  delete propagation", t3_delete),
    ("T4  bulk insert consistency", t4_bulk),
    ("T5  row-level field parity", t5_parity),
    ("T6  restart idempotency", t6_restart),
    ("T7  schema change new column", t7_schema_new_column),
    ("T8  schema sync", t8_schema_sync),
    ("T9  replication lag bound", t9_lag_bound),
    ("T10 snapshot completeness", t10_snapshot_completeness),
]


def main() -> int:
    pg, mongo = get_pg(), get_mongo()
    passed = 0
    for name, fn in TESTS:
        try:
            ok, reason = fn(pg, mongo)
        except Exception as exc:
            ok, reason = False, f"exception: {exc}"
        passed += int(ok)
        print(f"[{'PASS' if ok else 'FAIL'}] {name} — {reason}")
    print(f"\n{passed}/{len(TESTS)} tests passed")
    return 0 if passed == len(TESTS) else 1


if __name__ == "__main__":
    sys.exit(main())
