"""Central configuration. Every tunable knob lives here and is env-overridable so it
can be changed at deploy time (and live during the interview) without touching code.

The DESIGN.md tuning table maps each of these to its observed effect on lag/throughput.
"""
import os
from dataclasses import dataclass, field
from typing import List


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _str(name: str, default: str) -> str:
    return os.environ.get(name, default)


# Identity of the source tables we replicate, in FK-safe order (parents first).
# The primary-key column doubles as the MongoDB _id, giving us write idempotency.
@dataclass(frozen=True)
class Table:
    name: str          # PostgreSQL table name (schema-qualified as public.<name>)
    pk: str            # primary-key column -> MongoDB _id


TABLES: List[Table] = [
    Table(name="orders", pk="order_id"),
    Table(name="order_items", pk="item_id"),
]


@dataclass(frozen=True)
class Config:
    # --- Connections ---
    pg_dsn: str = field(default_factory=lambda: _str(
        "PG_DSN", "host=localhost port=5432 dbname=shop user=cdc password=cdc"))
    mongo_uri: str = field(default_factory=lambda: _str("MONGO_URI", "mongodb://localhost:27017"))
    mongo_db: str = field(default_factory=lambda: _str("MONGO_DB", "shop"))

    # --- Logical replication slot ---
    slot_name: str = field(default_factory=lambda: _str("SLOT_NAME", "cdc_slot"))

    # --- Source read (wal2json consumer) ---
    # How often the consumer wakes up to send standby status / drain feedback.
    standby_message_timeout: int = field(default_factory=lambda: _int("STANDBY_MESSAGE_TIMEOUT", 5))

    # --- Transport (in-process bounded queue) ---
    # Bounded queue = back-pressure: when full, the reader blocks and the WAL slot
    # retains data instead of dropping events.
    queue_maxsize: int = field(default_factory=lambda: _int("QUEUE_MAXSIZE", 50000))

    # --- Destination write ---
    write_batch_size: int = field(default_factory=lambda: _int("WRITE_BATCH_SIZE", 1000))
    # Max time (s) the writer waits to fill a batch before flushing a partial one.
    write_batch_linger: float = field(default_factory=lambda: float(os.environ.get("WRITE_BATCH_LINGER", 0.5)))
    write_concern_w: str = field(default_factory=lambda: _str("WRITE_CONCERN", "1"))
    write_retry_limit: int = field(default_factory=lambda: _int("WRITE_RETRY_LIMIT", 5))
    mongo_pool_size: int = field(default_factory=lambda: _int("MONGO_POOL_SIZE", 50))

    # --- Snapshot ---
    snapshot_chunk_size: int = field(default_factory=lambda: _int("SNAPSHOT_CHUNK_SIZE", 5000))

    # --- Schema registry ---
    # How long the live pipeline caches tracked columns before re-reading the
    # registry, so an operator's out-of-band `schema-sync` is picked up promptly.
    schema_cache_ttl: float = field(default_factory=lambda: float(os.environ.get("SCHEMA_CACHE_TTL", 5)))

    # --- Observability ---
    metrics_port: int = field(default_factory=lambda: _int("METRICS_PORT", 8000))
    metrics_log_interval: int = field(default_factory=lambda: _int("METRICS_LOG_INTERVAL", 5))

    # --- SLO (asserted by correctness test T9) ---
    lag_slo_seconds: float = field(default_factory=lambda: float(os.environ.get("LAG_SLO_SECONDS", 5)))


CONFIG = Config()

# Collection that stores pipeline metadata: the persisted LSN offset and the
# tracked-column schema registry. Single document per logical key.
META_COLLECTION = "_cdc_meta"
