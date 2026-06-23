"""Shared helpers for the correctness suite: connections, polling, and the canonical
value comparison used for field parity. Tests connect to the same PG/Mongo the
pipeline uses (localhost when run from the host; service names when run in-container).
"""
import time

from pipeline.config import CONFIG, TABLES
from pipeline.db import mongo_client, mongo_db, pg_connect, pg_table_columns
from pipeline.transform import coerce_value

PK_BY_TABLE = {t.name: t.pk for t in TABLES}


def get_pg():
    conn = pg_connect()
    conn.autocommit = True
    return conn


def get_mongo():
    return mongo_db(mongo_client())


def wait_until(predicate, timeout: float = 30.0, interval: float = 0.5) -> bool:
    """Poll predicate() until it returns truthy or the timeout elapses."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return bool(predicate())


def pg_row_canonical(pg, table: str, pk_value) -> dict:
    """Read a PG row and coerce every column to the same canonical form the pipeline
    writes, so it can be compared field-by-field with the Mongo document."""
    types = pg_table_columns(pg, table)
    cols = list(types.keys())
    with pg.cursor() as cur:
        cur.execute(f"SELECT {', '.join(cols)} FROM {table} WHERE {PK_BY_TABLE[table]} = %s",
                    (pk_value,))
        row = cur.fetchone()
    if row is None:
        return {}
    return {c: coerce_value(types[c], v) for c, v in zip(cols, row)}


def pg_count(pg, table: str) -> int:
    with pg.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        return cur.fetchone()[0]


def mongo_live_count(mongo, table: str) -> int:
    """Count documents that are not soft-deleted (mirrors live PG rows)."""
    return mongo[table].count_documents({"deleted_at": None})
