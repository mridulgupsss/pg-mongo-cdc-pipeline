"""Consistent initial snapshot (spec 4.1).

When the replication slot is created it exports a transaction snapshot at the slot's
consistent_point LSN. We read every source table inside a REPEATABLE READ transaction
pinned to that exact snapshot, so the snapshot reflects the database state at exactly
the LSN from which streaming will begin. Any change committed after that LSN is *not*
in the snapshot but *is* replayed by the stream -> seamless, lossless handoff.

Documents are built with the shared transform.build_document() so they are identical
to what the streaming path would produce, and stamped with the snapshot LSN so later
streamed updates (higher LSN) win via the LSN guard.
"""
import logging

import psycopg2
from pymongo import UpdateOne

from pipeline.config import CONFIG, TABLES
from pipeline.db import pg_table_columns
from pipeline.transform import build_document

log = logging.getLogger("cdc.snapshot")


def _snapshot_table(pg_conn, mongo_db, table_name: str, pk_col: str,
                    tracked: set, lsn: int) -> int:
    """COPY one table into Mongo via a server-side cursor, chunked. Returns row count.
    Uses upsert so a re-run (e.g. after a mid-snapshot crash) is idempotent."""
    types = pg_table_columns(pg_conn, table_name)
    collection = mongo_db[table_name]
    total = 0

    # Named (server-side) cursor streams rows instead of buffering all 500k+ in memory.
    with pg_conn.cursor(name=f"snap_{table_name}") as cur:
        cur.itersize = CONFIG.snapshot_chunk_size
        cur.execute(f"SELECT * FROM {table_name}")
        columns = [d.name for d in cur.description]

        batch = []
        for row in cur:
            values = dict(zip(columns, row))
            doc = build_document(values, types, pk_col, tracked, lsn)
            batch.append(UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True))
            if len(batch) >= CONFIG.snapshot_chunk_size:
                collection.bulk_write(batch, ordered=False)
                total += len(batch)
                batch = []
                log.info("snapshot %s: %d rows", table_name, total)
        if batch:
            collection.bulk_write(batch, ordered=False)
            total += len(batch)

    log.info("snapshot %s complete: %d rows", table_name, total)
    return total


def run_snapshot(snapshot_name: str, mongo_db, registry, lsn: int) -> dict:
    """Run the full consistent snapshot for all tables at the exported snapshot.
    Returns {table: row_count}. Tables are processed parents-first (TABLES order)."""
    counts = {}
    # A dedicated connection pinned to the exported snapshot.
    conn = psycopg2.connect(CONFIG.pg_dsn)
    try:
        conn.set_session(isolation_level=psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ)
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION SNAPSHOT %s", (snapshot_name,))

        for table in TABLES:
            registry.seed_if_absent(conn, table.name)
            tracked = registry.load(table.name)
            counts[table.name] = _snapshot_table(
                conn, mongo_db, table.name, table.pk, tracked, lsn)
        conn.commit()
    finally:
        conn.close()
    log.info("snapshot finished: %s", counts)
    return counts
