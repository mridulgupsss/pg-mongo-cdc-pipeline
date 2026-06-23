"""Thin connection helpers shared across the pipeline. Keeps connection/config
details in one place so the rest of the code reads cleanly."""
import psycopg2
import psycopg2.extras
from pymongo import MongoClient

from pipeline.config import CONFIG


def mongo_client() -> MongoClient:
    """A pymongo client honouring the configured pool size and write concern."""
    w: object = int(CONFIG.write_concern_w) if CONFIG.write_concern_w.isdigit() else CONFIG.write_concern_w
    return MongoClient(CONFIG.mongo_uri, maxPoolSize=CONFIG.mongo_pool_size, w=w)


def mongo_db(client: MongoClient):
    return client[CONFIG.mongo_db]


def pg_connect():
    """A standard (non-replication) PostgreSQL connection for snapshots/metadata."""
    return psycopg2.connect(CONFIG.pg_dsn)


def pg_table_columns(conn, table: str) -> dict:
    """Live column -> data_type map from information_schema, ordered by position.
    Used to seed/sync the schema registry and to type the snapshot rows."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table,),
        )
        return {name: dtype for name, dtype in cur.fetchall()}
