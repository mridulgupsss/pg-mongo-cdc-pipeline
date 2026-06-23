"""Airbyte-style schema-drift handling (spec 4.4).

The registry stores, per table, the set of *tracked* columns that the pipeline is
allowed to replicate. It is seeded once from the live PostgreSQL schema and then
only changes when an operator explicitly runs `schema-sync`.

Behaviour:
  - Columns present in PG but NOT in the registry are skipped during streaming and a
    throttled WARN is logged ("drift detected"). The pipeline never crashes.
  - `sync()` re-reads the live PG schema, adds the new columns to the registry, and
    backfills every existing Mongo document with a null for each newly-tracked column.
  - Columns dropped in PG are removed from the registry (we stop writing them).
"""
import logging
import time

from pipeline.config import CONFIG, META_COLLECTION
from pipeline.db import pg_table_columns

log = logging.getLogger("cdc.schema")


class SchemaRegistry:
    def __init__(self, db):
        self._col = db[META_COLLECTION]
        self._cache: dict = {}                 # table -> set(tracked columns)
        self._cache_at: dict = {}              # table -> monotonic time of last load
        self._warned: set = set()              # (table, column) already warned about

    def _doc_id(self, table: str) -> str:
        return f"schema:{table}"

    def load(self, table: str) -> set:
        """Tracked columns for a table. Cached with a short TTL so an operator's
        out-of-band `schema-sync` (a separate process) is picked up within seconds
        without auto-propagating drift on its own."""
        fresh = (table in self._cache
                 and time.monotonic() - self._cache_at[table] < CONFIG.schema_cache_ttl)
        if not fresh:
            doc = self._col.find_one({"_id": self._doc_id(table)})
            self._cache[table] = set(doc["columns"]) if doc else set()
            self._cache_at[table] = time.monotonic()
        return self._cache[table]

    def seed_if_absent(self, pg_conn, table: str) -> None:
        """On first ever run, register the current PG columns as the baseline."""
        if self._col.find_one({"_id": self._doc_id(table)}):
            return
        columns = list(pg_table_columns(pg_conn, table).keys())
        self._col.update_one(
            {"_id": self._doc_id(table)},
            {"$set": {"columns": columns}},
            upsert=True,
        )
        self._cache[table] = set(columns)
        self._cache_at[table] = time.monotonic()
        log.info("schema registry seeded for %s: %s", table, columns)

    def warn_drift(self, table: str, column: str) -> None:
        """Log once per (table, column) that an untracked column is being skipped."""
        key = (table, column)
        if key not in self._warned:
            self._warned.add(key)
            log.warning("schema drift detected: %s.%s present in PG but not synced; "
                        "skipping. Run `schema-sync` to propagate.", table, column)

    def sync(self, pg_conn, mongo_db, table: str) -> dict:
        """Align the registry + Mongo docs with the live PG schema. Returns a summary
        of added/removed columns. Idempotent — re-running is a no-op."""
        live = list(pg_table_columns(pg_conn, table).keys())
        tracked = self.load(table)
        added = [c for c in live if c not in tracked]
        removed = [c for c in tracked if c not in live]

        if added:
            # Backfill existing documents with null for each newly-tracked column.
            mongo_db[table].update_many(
                {},
                {"$set": {c: None for c in added}},
            )
        self._col.update_one(
            {"_id": self._doc_id(table)},
            {"$set": {"columns": live}},
            upsert=True,
        )
        self._cache[table] = set(live)
        self._cache_at[table] = time.monotonic()
        log.info("schema-sync %s: added=%s removed=%s", table, added, removed)
        return {"table": table, "added": added, "removed": removed}
