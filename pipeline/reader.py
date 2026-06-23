"""Logical-replication reader.

Owns the replication connection and the slot. Decodes wal2json format-v2 messages,
detects schema drift, transforms each change into a MongoOp, and enqueues it onto the
bounded queue (which provides back-pressure). After each message it feeds the writer's
durable LSN back to the slot so the slot only retains un-applied WAL.
"""
import json
import logging
from decimal import Decimal

import psycopg2
from psycopg2.extras import LogicalReplicationConnection, REPLICATION_LOGICAL

from pipeline.config import CONFIG, TABLES
from pipeline.transform import canonical_timestamp, change_to_op

log = logging.getLogger("cdc.reader")

_PK_BY_TABLE = {t.name: t.pk for t in TABLES}
_TABLE_NAMES = {t.name for t in TABLES}

# wal2json options. add-tables scopes decoding to our tables; format-version 2 emits
# one change per message; include-types/timestamp give us coercion + commit time.
_WAL2JSON_OPTIONS = {
    "format-version": "2",
    "include-types": "1",
    "include-timestamp": "1",
    "actions": "insert,update,delete",
    "add-tables": ",".join(f"public.{t.name}" for t in TABLES),
}


def lsn_to_int(lsn: str) -> int:
    hi, lo = lsn.split("/")
    return (int(hi, 16) << 32) | int(lo, 16)


class Reader:
    def __init__(self, q, registry, metrics, writer):
        self._q = q
        self._registry = registry
        self._metrics = metrics
        self._writer = writer
        self._conn = psycopg2.connect(CONFIG.pg_dsn, connection_factory=LogicalReplicationConnection)
        self._cur = self._conn.cursor()
        self._running = False

    # --- slot lifecycle ---
    def slot_exists(self) -> bool:
        with psycopg2.connect(CONFIG.pg_dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                        (CONFIG.slot_name,))
            return cur.fetchone() is not None

    def create_slot(self) -> tuple:
        """Create the slot and return (consistent_lsn:int, snapshot_name:str). The
        exported snapshot is valid until the next command on this connection, so the
        caller must run the snapshot before start_streaming()."""
        self._cur.execute(
            f"CREATE_REPLICATION_SLOT {CONFIG.slot_name} LOGICAL wal2json")
        _slot, consistent_point, snapshot_name, _plugin = self._cur.fetchone()
        log.info("created slot %s at %s (snapshot %s)",
                 CONFIG.slot_name, consistent_point, snapshot_name)
        return lsn_to_int(consistent_point), snapshot_name

    def drop_slot(self) -> None:
        if self.slot_exists():
            self._cur.execute(f"DROP_REPLICATION_SLOT {CONFIG.slot_name}")
            log.info("dropped slot %s", CONFIG.slot_name)

    # --- streaming ---
    def start_streaming(self) -> None:
        """Begin consuming from the slot and pushing MongoOps to the queue. Blocks
        until stop() is called or the connection drops."""
        self._cur.start_replication(
            slot_name=CONFIG.slot_name,
            slot_type=REPLICATION_LOGICAL,
            decode=True,
            options=_WAL2JSON_OPTIONS,
            status_interval=CONFIG.standby_message_timeout,
        )
        self._running = True
        log.info("streaming from slot %s", CONFIG.slot_name)
        self._cur.consume_stream(self._on_message)

    def _on_message(self, msg) -> None:
        # parse_float=Decimal preserves NUMERIC fidelity: wal2json emits numerics as
        # unquoted JSON numbers, and plain json.loads would coerce them to float and
        # drop trailing zeros (e.g. 1419.20 -> 1419.2), breaking field parity.
        change = json.loads(msg.payload, parse_float=Decimal)
        op = self._build_op(change, msg.data_start)
        if op is not None:
            commit_ts = canonical_timestamp(change["timestamp"]) if change.get("timestamp") else None
            # Blocks here when the queue is full -> natural back-pressure on the WAL.
            self._q.put((op, commit_ts))
            self._metrics.on_read(commit_ts)
        self._confirm(msg)

    def _confirm(self, msg) -> None:
        """Advance the slot's confirmed_flush_lsn. We confirm up to the writer's durable
        LSN; additionally, when the queue is fully drained (everything we've read is
        applied), we confirm up to the end of received WAL — this releases WAL that
        logical decoding filtered out (other tables/activity), preventing slot bloat."""
        flush = self._writer.flushed_lsn
        if self._q.empty():
            flush = max(flush, msg.wal_end)
        if flush:
            msg.cursor.send_feedback(flush_lsn=flush)

    def _build_op(self, change: dict, lsn: int):
        table = change.get("table")
        if table not in _TABLE_NAMES:
            return None
        self._warn_on_drift(change, table)
        return change_to_op(change, _PK_BY_TABLE[table], self._registry.load(table), lsn)

    def _warn_on_drift(self, change: dict, table: str) -> None:
        """Surface (and skip) any column present in PG but not yet synced."""
        if change.get("action") not in ("I", "U"):
            return
        tracked = self._registry.load(table)
        for col in change.get("columns", []):
            if col["name"] not in tracked:
                self._registry.warn_drift(table, col["name"])

    def stop(self) -> None:
        self._running = False
        try:
            self._conn.close()
        except Exception:
            pass
