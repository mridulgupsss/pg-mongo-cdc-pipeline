"""Seed the source tables with a realistic volume of data (spec 3.2):
  - >= 500,000 orders (the primary table)
  - 2..5 order_items per order (the child table)

Uses COPY for throughput. Run once against an empty schema:
    python scripts/seed.py [--orders 500000]

Reproducible: the RNG is fixed-seeded so a re-run produces the same data.
"""
import argparse
import io
import random
import sys
from datetime import datetime, timedelta, timezone

# Allow running as a plain script (python scripts/seed.py) as well as a module.
sys.path.insert(0, ".")
from pipeline.db import pg_connect  # noqa: E402

random.seed(42)

STATUSES = ["pending", "paid", "shipped", "delivered", "cancelled"]
PRODUCTS = ["Wireless Mouse", "Mechanical Keyboard", "USB-C Cable", "Laptop Stand",
            "Noise-Cancelling Headphones", "Webcam 1080p", "Desk Lamp", "Monitor Arm",
            "Ergonomic Chair", "Standing Desk Mat"]
COPY_CHUNK = 50_000
_BASE_TIME = datetime(2024, 1, 1, tzinfo=timezone.utc)


def _copy_rows(cur, table: str, columns: str, rows) -> None:
    """Stream an iterable of CSV-row strings into Postgres via COPY."""
    buf = io.StringIO()
    buf.writelines(rows)
    buf.seek(0)
    cur.copy_expert(f"COPY {table} ({columns}) FROM STDIN WITH (FORMAT csv)", buf)


def _order_rows(start: int, count: int):
    for i in range(start, start + count):
        ts = (_BASE_TIME + timedelta(seconds=i)).isoformat()
        email = f"customer{i}@example.com"
        amount = f"{random.uniform(5, 2000):.2f}"
        status = random.choice(STATUSES)
        is_paid = random.random() < 0.6
        yield f"{email},{amount},{status},{is_paid},{ts},{ts}\n"


def _item_rows(order_id: int):
    for _ in range(random.randint(2, 5)):
        product = random.choice(PRODUCTS)
        qty = random.randint(1, 4)
        price = f"{random.uniform(1, 500):.2f}"
        is_gift = random.random() < 0.1
        ts = (_BASE_TIME + timedelta(seconds=order_id)).isoformat()
        yield f"{order_id},{product},{qty},{price},{is_gift},{ts}\n"


def seed_orders(conn, total: int) -> None:
    done = 0
    with conn.cursor() as cur:
        while done < total:
            batch = min(COPY_CHUNK, total - done)
            _copy_rows(cur, "orders",
                       "customer_email,total_amount,status,is_paid,created_at,updated_at",
                       _order_rows(done + 1, batch))
            done += batch
            conn.commit()
            print(f"orders: {done}/{total}", flush=True)


def seed_items(conn, total_orders: int) -> None:
    done = 0
    with conn.cursor() as cur:
        order_id = 1
        while order_id <= total_orders:
            chunk_end = min(order_id + COPY_CHUNK, total_orders + 1)
            rows = (row for oid in range(order_id, chunk_end) for row in _item_rows(oid))
            _copy_rows(cur, "order_items",
                       "order_id,product_name,quantity,unit_price,is_gift,created_at", rows)
            done += (chunk_end - order_id)
            order_id = chunk_end
            conn.commit()
            print(f"order_items: parents {done}/{total_orders}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--orders", type=int, default=500_000)
    args = parser.parse_args()

    conn = pg_connect()
    try:
        print(f"seeding {args.orders} orders ...", flush=True)
        seed_orders(conn, args.orders)
        print("seeding order_items ...", flush=True)
        seed_items(conn, args.orders)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM orders")
            n_orders = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM order_items")
            n_items = cur.fetchone()[0]
        print(f"done. orders={n_orders} order_items={n_items}", flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
