"""Pipeline entry point and CLI.

  python -m pipeline.main run           # snapshot (first run) then stream forever
  python -m pipeline.main schema-sync   # align Mongo schema with current PG schema
  python -m pipeline.main status        # print a one-shot metrics snapshot

The `run` command orchestrates the snapshot->stream handoff:
  1. If never initialised (or the slot is gone), drop+recreate the slot. Creating the
     slot exports a snapshot at its consistent LSN.
  2. Load that consistent snapshot into Mongo (snapshot.run_snapshot).
  3. Start streaming from the slot. The slot begins exactly at the snapshot LSN, so no
     committed change between snapshot and stream is lost.
"""
import argparse
import logging
import queue
import signal
import sys

from pipeline.config import CONFIG, META_COLLECTION, TABLES
from pipeline.db import mongo_client, mongo_db, pg_connect
from pipeline.metrics import Metrics, MetricsReporter
from pipeline.offset import OffsetStore
from pipeline.reader import Reader
from pipeline.schema_registry import SchemaRegistry
from pipeline.snapshot import run_snapshot
from pipeline.writer import Writer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("cdc.main")

_SNAPSHOT_STATE_ID = "snapshot"


def _snapshot_done(db) -> bool:
    doc = db[META_COLLECTION].find_one({"_id": _SNAPSHOT_STATE_ID})
    return bool(doc and doc.get("done"))


def _mark_snapshot_done(db, counts: dict) -> None:
    db[META_COLLECTION].update_one(
        {"_id": _SNAPSHOT_STATE_ID},
        {"$set": {"done": True, "counts": counts}},
        upsert=True,
    )


def _initialise(db, reader, registry, offset) -> None:
    """Snapshot-to-stream handoff. Idempotent: skips the snapshot on a clean restart."""
    if _snapshot_done(db) and reader.slot_exists():
        log.info("snapshot already complete and slot present; resuming stream")
        return

    log.info("initialising: (re)creating slot and running consistent snapshot")
    reader.drop_slot()
    lsn, snapshot_name = reader.create_slot()
    counts = run_snapshot(snapshot_name, db, registry, lsn)
    offset.save(lsn)
    _mark_snapshot_done(db, counts)
    log.info("snapshot complete at lsn=%s counts=%s", lsn, counts)


def cmd_run() -> None:
    client = mongo_client()
    db = mongo_db(client)

    q: queue.Queue = queue.Queue(maxsize=CONFIG.queue_maxsize)
    metrics = Metrics()
    offset = OffsetStore(db)
    registry = SchemaRegistry(db)
    writer = Writer(q, db, offset, metrics)
    reader = Reader(q, registry, metrics, writer)
    reporter = MetricsReporter(metrics, q)

    _initialise(db, reader, registry, offset)

    writer.start()
    reporter.start()

    def shutdown(*_):
        log.info("shutdown requested; draining")
        reader.stop()
        writer.stop()
        writer.join(timeout=30)
        reporter.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        reader.start_streaming()
    except Exception as exc:  # connection closed on shutdown raises here
        log.warning("stream ended: %s", exc)


def cmd_schema_sync(tables) -> None:
    client = mongo_client()
    db = mongo_db(client)
    registry = SchemaRegistry(db)
    targets = tables or [t.name for t in TABLES]
    with pg_connect() as pg:
        for table in targets:
            result = registry.sync(pg, db, table)
            print(f"schema-sync {result['table']}: "
                  f"added={result['added']} removed={result['removed']}")


def cmd_status() -> None:
    client = mongo_client()
    db = mongo_db(client)
    metrics = Metrics()
    doc = db[META_COLLECTION].find_one({"_id": "offset"})
    last_lsn = doc.get("lsn") if doc else None
    with pg_connect() as pg:
        print({"last_applied_lsn": last_lsn, "lag_bytes": metrics.lag_bytes(pg)})


def main() -> None:
    parser = argparse.ArgumentParser(prog="pipeline")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run", help="run snapshot + streaming pipeline")
    p_sync = sub.add_parser("schema-sync", help="align Mongo schema with current PG schema")
    p_sync.add_argument("tables", nargs="*", help="tables to sync (default: all)")
    sub.add_parser("status", help="print a metrics snapshot and exit")

    args = parser.parse_args()
    if args.command == "run":
        cmd_run()
    elif args.command == "schema-sync":
        cmd_schema_sync(args.tables)
    elif args.command == "status":
        cmd_status()


if __name__ == "__main__":
    main()
