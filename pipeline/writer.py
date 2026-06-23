"""MongoDB writer: drains the bounded queue, coalesces changes into batches, and
applies them as idempotent, LSN-guarded bulk upserts.

Delivery guarantee: the writer only advances `flushed_lsn` (which the reader feeds
back to the slot) *after* a batch is durably written. So a crash between write and
feedback merely replays already-written events, which are idempotent (upsert keyed by
PK + the monotonic _lsn guard). This is at-least-once with effectively-once results.
"""
import logging
import queue as queue_mod
import threading
import time

from pymongo import UpdateOne
from pymongo.errors import PyMongoError

from pipeline.config import CONFIG
from pipeline.transform import build_pipeline_update

log = logging.getLogger("cdc.writer")

# Sentinel pushed onto the queue to tell the writer to drain and exit.
STOP = object()


class Writer(threading.Thread):
    def __init__(self, q: queue_mod.Queue, mongo_db, offset_store, metrics):
        super().__init__(daemon=True)
        self._q = q
        self._db = mongo_db
        self._offset = offset_store
        self._metrics = metrics
        self.flushed_lsn = 0          # last durably-written LSN; read by the reader
        self._stopped = False

    # --- batching ---
    def _collect_batch(self):
        """Block for the first item, then opportunistically fill up to
        write_batch_size within the linger window. Returns (ops, stop_seen)."""
        batch = []
        try:
            first = self._q.get(timeout=1.0)
        except queue_mod.Empty:
            return batch, False
        if first is STOP:
            return batch, True
        batch.append(first)

        deadline = time.monotonic() + CONFIG.write_batch_linger
        while len(batch) < CONFIG.write_batch_size:
            timeout = deadline - time.monotonic()
            if timeout <= 0:
                break
            try:
                item = self._q.get(timeout=timeout)
            except queue_mod.Empty:
                break
            if item is STOP:
                return batch, True
            batch.append(item)
        return batch, False

    # --- writing ---
    def _apply(self, batch) -> None:
        """Group ops by collection and bulk-write them with bounded retry. Each op is
        an LSN-guarded conditional upsert, so the batch is order-independent and safe
        to retry wholesale on failure."""
        per_table: dict = {}
        max_lsn = 0
        last_commit = None
        for op, commit_ts in batch:
            per_table.setdefault(op.table, []).append(
                UpdateOne({"_id": op.pk_value},
                          build_pipeline_update(op.set_fields, op.lsn),
                          upsert=True))
            max_lsn = max(max_lsn, op.lsn)
            last_commit = commit_ts or last_commit

        for table, ops in per_table.items():
            self._bulk_write_with_retry(table, ops)

        # Durable now -> safe to advance the offset the slot will be told about.
        self.flushed_lsn = max(self.flushed_lsn, max_lsn)
        self._offset.save(self.flushed_lsn)
        self._metrics.on_write(len(batch), max_lsn, last_commit)

    def _bulk_write_with_retry(self, table: str, ops) -> None:
        attempt = 0
        while True:
            try:
                self._db[table].bulk_write(ops, ordered=False)
                return
            except PyMongoError as exc:
                attempt += 1
                if attempt > CONFIG.write_retry_limit:
                    log.error("bulk_write to %s failed after %d attempts: %s",
                              table, attempt, exc)
                    raise
                backoff = min(2 ** attempt * 0.1, 5.0)
                log.warning("bulk_write to %s failed (attempt %d), retrying in %.1fs: %s",
                            table, attempt, backoff, exc)
                time.sleep(backoff)

    def run(self):
        while not self._stopped:
            batch, stop_seen = self._collect_batch()
            if batch:
                self._apply(batch)
            if stop_seen:
                break
        log.info("writer stopped; flushed_lsn=%s", self.flushed_lsn)

    def stop(self):
        self._stopped = True
        self._q.put(STOP)
