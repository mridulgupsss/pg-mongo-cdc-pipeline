"""Observability: a small set of counters, a periodic structured log line, and a
plaintext /metrics HTTP endpoint (bonus #9). This is logs + a text endpoint only —
NOT a UI/dashboard (ground rule, spec 10).

Two lag signals (spec 5.2):
  - lag_seconds: wall-clock staleness = now - commit time of the last applied change.
  - lag_bytes:   WAL backlog = pg_current_wal_lsn - slot.confirmed_flush_lsn.
Counters also expose where the bottleneck is (read rate vs queue depth vs write rate).
"""
import logging
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

from pipeline.config import CONFIG
from pipeline.db import pg_connect

log = logging.getLogger("cdc.metrics")


class Metrics:
    def __init__(self):
        self._lock = threading.Lock()
        self.events_read = 0
        self.events_written = 0
        self.batches_written = 0
        self.queue_depth = 0
        self.last_applied_lsn = 0
        self.last_read_ts = None          # commit time of last change READ from WAL
        self.last_written_ts = None       # commit time of last change WRITTEN to Mongo
        self._start = time.monotonic()

    # --- mutators (cheap, lock-guarded) ---
    def on_read(self, commit_ts=None, n: int = 1):
        with self._lock:
            self.events_read += n
            if commit_ts:
                self.last_read_ts = commit_ts

    def on_write(self, n: int, lsn: int, commit_ts):
        with self._lock:
            self.events_written += n
            self.batches_written += 1
            self.last_applied_lsn = max(self.last_applied_lsn, lsn)
            if commit_ts:
                self.last_written_ts = commit_ts

    def set_queue_depth(self, depth: int):
        with self._lock:
            self.queue_depth = depth

    # --- derived signals ---
    def lag_seconds(self) -> float:
        """Event-time replication lag: how far the last change applied to Mongo trails
        the last change read from the WAL. Naturally ~0 when caught up/idle (reader and
        writer converge on the same change), and positive while the writer is behind."""
        with self._lock:
            read_ts, written_ts = self.last_read_ts, self.last_written_ts
        if not read_ts or not written_ts:
            return 0.0
        gap = (datetime.fromisoformat(read_ts) - datetime.fromisoformat(written_ts)).total_seconds()
        return max(0.0, gap)

    def lag_bytes(self, pg_conn) -> int:
        """WAL bytes the slot is behind the current write position."""
        try:
            with pg_conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) "
                    "FROM pg_replication_slots WHERE slot_name = %s",
                    (CONFIG.slot_name,),
                )
                row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else 0
        except Exception:
            pg_conn.rollback()
            return -1

    def snapshot(self, pg_conn) -> dict:
        with self._lock:
            elapsed = max(1e-6, time.monotonic() - self._start)
            base = {
                "events_read": self.events_read,
                "events_written": self.events_written,
                "batches_written": self.batches_written,
                "queue_depth": self.queue_depth,
                "read_rate": round(self.events_read / elapsed, 1),
                "write_rate": round(self.events_written / elapsed, 1),
                "last_applied_lsn": self.last_applied_lsn,
            }
        base["lag_seconds"] = round(self.lag_seconds(), 3)
        base["lag_bytes"] = self.lag_bytes(pg_conn)
        return base


class MetricsReporter(threading.Thread):
    """Background thread: logs the metrics line every N seconds and serves /metrics."""

    def __init__(self, metrics: Metrics, queue):
        super().__init__(daemon=True)
        self._metrics = metrics
        self._queue = queue
        self._pg = pg_connect()
        self._stop = threading.Event()
        self._http = _make_server(metrics, self._pg)

    def run(self):
        threading.Thread(target=self._http.serve_forever, daemon=True).start()
        while not self._stop.wait(CONFIG.metrics_log_interval):
            self._metrics.set_queue_depth(self._queue.qsize())
            snap = self._metrics.snapshot(self._pg)
            log.info("metrics %s", snap)

    def stop(self):
        self._stop.set()
        self._http.shutdown()


def _make_server(metrics: Metrics, pg_conn) -> HTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            snap = metrics.snapshot(pg_conn)
            body = "".join(f"cdc_{k} {v}\n" for k, v in snap.items()).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):  # silence default request logging
            pass

    return HTTPServer(("0.0.0.0", CONFIG.metrics_port), Handler)
