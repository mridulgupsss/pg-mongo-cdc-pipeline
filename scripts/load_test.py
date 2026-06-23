"""Continuous write workload against PostgreSQL (spec 5.1).

    python scripts/load_test.py --duration 120

Targets (per second, tunable via flags):
    INSERT  3000-5000 rows/sec  (orders + their order_items, FK-consistent)
    UPDATE  1000-2000 rows/sec  (random orders, 1-3 columns each)
    DELETE   200-500  rows/sec  (random order_items; child rows have no FK dependents)

Each operation category runs in its own paced thread. Prints a summary at the end:
total ops, ops/sec, and error count.
"""
import argparse
import random
import threading
import time
from dataclasses import dataclass, field

import psycopg2.extras

from pipeline.db import pg_connect

random.seed(1234)
STATUSES = ["pending", "paid", "shipped", "delivered", "cancelled"]
PRODUCTS = ["Wireless Mouse", "Mechanical Keyboard", "USB-C Cable", "Laptop Stand"]
CYCLES_PER_SEC = 20             # pacing granularity per worker


@dataclass
class Stats:
    inserts: int = 0
    updates: int = 0
    deletes: int = 0
    errors: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def add(self, *, inserts=0, updates=0, deletes=0, errors=0):
        with self._lock:
            self.inserts += inserts
            self.updates += updates
            self.deletes += deletes
            self.errors += errors


def _max_id(conn, table, col) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COALESCE(max({col}), 0) FROM {table}")
        return cur.fetchone()[0]


def _paced(stop: threading.Event, rate: int, do_batch) -> None:
    """Call do_batch(batch_size) ~CYCLES_PER_SEC times/sec to approximate `rate`."""
    if rate <= 0:
        return
    batch = max(1, rate // CYCLES_PER_SEC)
    interval = 1.0 / CYCLES_PER_SEC
    while not stop.is_set():
        start = time.monotonic()
        do_batch(batch)
        sleep = interval - (time.monotonic() - start)
        if sleep > 0:
            stop.wait(sleep)


def insert_worker(stop, stats, rate):
    conn = pg_connect()
    conn.autocommit = True

    def do_batch(batch):
        try:
            order_rows = [
                (f"load{random.randint(0, 10**9)}@example.com",
                 round(random.uniform(5, 2000), 2), random.choice(STATUSES),
                 random.random() < 0.6)
                for _ in range(batch)
            ]
            with conn.cursor() as cur:
                ids = psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO orders (customer_email, total_amount, status, is_paid) "
                    "VALUES %s RETURNING order_id",
                    order_rows, fetch=True)
                item_rows = [
                    (oid[0], random.choice(PRODUCTS), random.randint(1, 4),
                     round(random.uniform(1, 500), 2), random.random() < 0.1)
                    for oid in ids for _ in range(random.randint(2, 3))
                ]
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO order_items (order_id, product_name, quantity, unit_price, is_gift) "
                    "VALUES %s", item_rows)
            stats.add(inserts=len(order_rows) + len(item_rows))
        except Exception:
            stats.add(errors=1)

    _paced(stop, rate, do_batch)
    conn.close()


def update_worker(stop, stats, rate, max_order_id):
    conn = pg_connect()
    conn.autocommit = True
    columns = [("status", lambda: random.choice(STATUSES)),
               ("is_paid", lambda: random.random() < 0.6),
               ("total_amount", lambda: round(random.uniform(5, 2000), 2))]

    def do_batch(batch):
        try:
            chosen = random.sample(columns, random.randint(1, 3))   # 1-3 columns this batch
            set_clause = ", ".join(f"{c} = %s" for c, _ in chosen) + ", updated_at = now()"
            stmts = [
                ([gen() for _, gen in chosen] + [random.randint(1, max_order_id)])
                for _ in range(batch)
            ]
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(
                    cur, f"UPDATE orders SET {set_clause} WHERE order_id = %s", stmts)
            stats.add(updates=batch)
        except Exception:
            stats.add(errors=1)

    _paced(stop, rate, do_batch)
    conn.close()


def delete_worker(stop, stats, rate, max_item_id):
    conn = pg_connect()
    conn.autocommit = True

    def do_batch(batch):
        try:
            ids = [random.randint(1, max_item_id) for _ in range(batch)]
            with conn.cursor() as cur:
                cur.execute("DELETE FROM order_items WHERE item_id = ANY(%s)", (ids,))
                deleted = cur.rowcount
            stats.add(deletes=max(0, deleted))
        except Exception:
            stats.add(errors=1)

    _paced(stop, rate, do_batch)
    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=120)
    parser.add_argument("--insert-rate", type=int, default=4000)
    parser.add_argument("--update-rate", type=int, default=1500)
    parser.add_argument("--delete-rate", type=int, default=350)
    args = parser.parse_args()

    probe = pg_connect()
    max_order_id = _max_id(probe, "orders", "order_id")
    max_item_id = _max_id(probe, "order_items", "item_id")
    probe.close()
    if max_order_id == 0:
        raise SystemExit("no data found; run scripts/seed.py first")

    stats = Stats()
    stop = threading.Event()
    threads = [
        threading.Thread(target=insert_worker, args=(stop, stats, args.insert_rate)),
        threading.Thread(target=update_worker, args=(stop, stats, args.update_rate, max_order_id)),
        threading.Thread(target=delete_worker, args=(stop, stats, args.delete_rate, max_item_id)),
    ]

    print(f"running load for {args.duration}s "
          f"(insert={args.insert_rate} update={args.update_rate} delete={args.delete_rate} /s targets)")
    start = time.monotonic()
    for t in threads:
        t.start()
    stop.wait(args.duration)
    stop.set()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - start

    total = stats.inserts + stats.updates + stats.deletes
    print("\n=== load test summary ===")
    print(f"duration:        {elapsed:.1f}s")
    print(f"inserts:         {stats.inserts}  ({stats.inserts / elapsed:.0f}/s)")
    print(f"updates:         {stats.updates}  ({stats.updates / elapsed:.0f}/s)")
    print(f"deletes:         {stats.deletes}  ({stats.deletes / elapsed:.0f}/s)")
    print(f"total ops:       {total}  ({total / elapsed:.0f}/s)")
    print(f"errors:          {stats.errors}")


if __name__ == "__main__":
    main()
