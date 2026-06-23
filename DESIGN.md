# DESIGN — Custom CDC Pipeline (PostgreSQL → MongoDB)

> **The numbers in §3 are measured**, on Docker Desktop (Apple Silicon, single
> pipeline process) against a 500k-order / 1.75M-item dataset. The correctness suite
> passes 10/10 deterministically (two consecutive runs identical). Reproduce with the
> commands in [§3.5 How to measure](#35-how-to-measure); exact figures vary with
> hardware.

---

## 7.1 Architecture

```
   ┌──────────────────────────── docker compose up ────────────────────────────┐
   │                                                                            │
   │   ┌──────────┐                                                             │
   │   │ Postgres │  WAL records                                                │
   │   │  (source)│────────────┐                                               │
   │   └────┬─────┘            │ logical decoding (wal2json output plugin)      │
   │        │ exported         ▼                                               │
   │        │ snapshot   ┌─────────────┐  JSON change rows   ┌──────────────┐  │
   │        │ (COPY)     │   Reader     │  (format-version 2) │  bounded     │  │
   │        └───────────▶│ psycopg2     │────────────────────▶│  queue       │  │
   │                     │ LogicalRepl  │   (op, commit_ts)    │ (in-process) │  │
   │                     └──────┬───────┘                      └──────┬───────┘  │
   │            send_feedback(  │ flush_lsn                           │ batches   │
   │            confirm LSN) ◀──┘                                     ▼           │
   │                                                          ┌──────────────┐   │
   │                                                          │   Writer     │   │
   │                                                          │ bulk_write   │   │
   │                                                          │ (upsert,     │   │
   │                                                          │  LSN-guarded)│   │
   │                                                          └──────┬───────┘   │
   │                                                                 │ BSON ops  │
   │                                                                 ▼           │
   │                                                          ┌──────────────┐   │
   │                                                          │   MongoDB    │   │
   │                                                          │ orders,      │   │
   │                                                          │ order_items, │   │
   │                                                          │ _cdc_meta    │   │
   │                                                          └──────────────┘   │
   └────────────────────────────────────────────────────────────────────────────┘

Arrow data formats:
  Postgres → Reader : Postgres WAL → wal2json JSON (one change object per message)
  Reader   → Queue  : (MongoOp dataclass, commit_ts)   [in-process Python objects]
  Writer   → Mongo  : pymongo UpdateOne upserts (aggregation-pipeline $set), BSON
  Writer   → Reader : confirmed flush LSN (int), fed back to the slot
```

Components: **Postgres + wal2json** (capture), **Reader** (decode + transform +
drift detection), **bounded queue** (transport / back-pressure), **Writer** (batched
idempotent upserts), **MongoDB** (destination + `_cdc_meta` for offset & schema
registry), **MetricsReporter** (logs + `/metrics`).

## 7.2 How the pipeline works

**From a committed PG transaction to a Mongo document**

1. A transaction commits in Postgres. Its row changes are written to the WAL.
2. The logical decoding slot, using `wal2json`, turns each change into a JSON object:
   `{"action":"I|U|D","table":...,"columns":[{name,type,value}...],"identity":[...]}`.
3. The **Reader** (`psycopg2` `LogicalReplicationConnection.consume_stream`) receives
   one message per change. It:
   - filters to our tables (slot `add-tables` option already scopes this),
   - warns on any column not in the schema registry (drift), skipping it,
   - calls the pure `transform.change_to_op()` → a `MongoOp` (PK, canonicalised
     `$set` fields, scalar LSN, soft-delete flag),
   - enqueues `(op, commit_ts)` onto the **bounded queue** (`queue.Queue`).
4. The **Writer** drains up to `WRITE_BATCH_SIZE` ops (or until `WRITE_BATCH_LINGER`
   elapses), groups them by collection, and issues one `bulk_write` per collection.
   Each op is an LSN-guarded conditional upsert (see idempotency below).
5. After the batch is durably written, the Writer sets `flushed_lsn` and mirrors it
   into `_cdc_meta`. On the next message the Reader calls `send_feedback(flush_lsn=…)`,
   advancing the slot's `confirmed_flush_lsn`.

**Offset tracking & persistence.** The authoritative offset is the replication slot's
`confirmed_flush_lsn`, persisted by Postgres itself. We only confirm an LSN *after* the
corresponding writes are durable in Mongo, so the slot never advances past un-applied
data. On restart, `start_replication` resumes from the slot's confirmed position — no
reprocessing of already-confirmed events, and no missed events. We additionally mirror
the last-applied LSN into `_cdc_meta` for observability (the lag metric reads it).

**Snapshot → stream handoff (no loss).** Creating the slot
(`CREATE_REPLICATION_SLOT … LOGICAL wal2json`) returns a *consistent point* LSN and an
*exported snapshot name*. The snapshot reader opens a separate `REPEATABLE READ`
transaction, runs `SET TRANSACTION SNAPSHOT <name>`, and `COPY`s every table at exactly
that LSN. Streaming then begins from the slot, which starts at the same consistent
point. Therefore: every change *before* the point is in the snapshot; every change
*after* it is replayed by the stream; nothing in between is lost. Snapshot documents
are stamped with the consistent-point LSN, so any later streamed update (higher LSN)
wins via the LSN guard. The handoff is idempotent — `_cdc_meta.snapshot.done` lets a
clean restart skip the snapshot; a crash *during* the initial snapshot re-creates the
slot and redoes the snapshot (the only case that re-reads, and it is not "normal
operation").

**Why logical replication (vs alternatives).**
- *vs trigger-based audit table:* triggers add synchronous write overhead on the hot
  path (every INSERT/UPDATE/DELETE does extra work inside the user's transaction),
  which directly hurts the 3–5k writes/s target. Logical decoding reads the WAL
  asynchronously — zero added latency to source writes.
- *vs polling `updated_at`:* cannot capture DELETEs, misses intermediate states
  between polls, and needs a full-table scan per poll. Not real CDC.
- Logical replication gives an exact, ordered, durable stream with a built-in,
  persistent offset (the slot) — the cleanest answer to "never lose events, never
  re-sync."

**Idempotency / at-least-once.** Delivery is at-least-once: a crash between writing a
batch and confirming its LSN replays that batch. Duplicates are absorbed because every
write is an **upsert keyed by the PG primary key (= Mongo `_id`)** — re-applying an
event produces the identical document. The writes also carry an **LSN guard**: each
field is set via an aggregation-pipeline `$cond` that only applies when the incoming
LSN exceeds the stored `_lsn` (missing `_lsn` treated as `-1`). This makes writes
monotonic and order-independent, so even a stale replay can never regress a newer value.

## 7.3 Staleness & resilience analysis

Lag is measured two ways (`/metrics`): `lag_seconds` = event-time gap between the last
change **read** from the WAL and the last change **written** to Mongo (≈0 when caught
up, bounded by queue depth under load); `lag_bytes` = `pg_current_wal_lsn −
slot.confirmed_flush_lsn` (raw WAL backlog — the better saturation signal).

Initial snapshot: **500k orders + 1.75M order_items in ~35 s** (~64k rows/s via COPY).

### 3.1 Replication lag (measured)

| Load (source rate) | `lag_seconds` | `queue_depth` | Behaviour |
|---|---|---|---|
| Idle | **0.0** | 0 | caught up |
| Spec / 1× (~4–5k insert rows/s + 1.5k upd + 350 del) | **0.05 – 0.11 s** | ~0 | writer keeps up |
| 2× spec (T9 end-to-end sentinel) | **0.51 s** (max) | small spikes | well under SLO |
| ~14.7k ops/s (3× spec) | **0.05 – 0.11 s** | mostly 0, spikes to ~360 | still sustained |
| ~73k ops/s (overload) | **~0.66 s** (bounded) | pinned at 50000 cap | back-pressure; soft-fail |

### 3.2 Maximum sustainable throughput (measured)

The **writer is the limiter** (Mongo `bulk_write` round-trips). The pipeline sustains
**≳15k changes/s** with the queue near-empty and sub-200ms lag. At a ~73k changes/s
source rate (2.87M insert rows in 45s) the writer saturated: the queue pinned at
`QUEUE_MAXSIZE` (50000), `lag_bytes` climbed into the hundreds of MB, and a WAL backlog
accumulated — **but 0 errors and no crash**. So the unbounded-growth knee for this
single-process configuration sits between ~15k and ~73k changes/s.

### 3.3 Bottleneck identification (evidence)

Under the 73k overload, `read_rate ≈ write_rate` while `queue_depth` stayed pinned at
the 50000 cap — i.e. the reader was blocked on `queue.put` waiting for the writer, not
the other way round. That is the signature of a **writer-bound** pipeline (Mongo
round-trips dominate). Levers: ↑ `WRITE_BATCH_SIZE`, `w=1`, ↑ `MONGO_POOL_SIZE`, and
ultimately sharding the writer (§3.6). `REPLICA IDENTITY FULL` inflates WAL volume —
a reader-side co-factor (§7.4).

### 3.4 Failure & recovery behaviour (measured)

The pipeline **fails soft**. Under the 73k overload the bounded queue filled, the
reader blocked on `queue.put`, and the slot retained WAL — lag grew but **no events
dropped and nothing crashed**. After the burst ended it **self-healed**: the queue
drained back to 0 and `lag_seconds` returned to 0 within **~60 s** (it had absorbed a
~3M-event backlog). The only "hard" risks are external (Mongo down, slot invalidated) —
see §7.5.

**SLO (asserted by T9):** under 2× normal load, end-to-end propagation lag ≤ **5 s**
(`LAG_SLO_SECONDS`). Measured T9 max: **0.51 s** — ~10× margin.

### 3.5 How to measure

```bash
docker compose up --build -d
docker compose exec pipeline python scripts/seed.py --orders 500000
# watch lag while load runs:
watch -n1 'curl -s localhost:8000 | grep -E "cdc_lag|cdc_queue|cdc_.*_rate"'
docker compose exec pipeline python scripts/load_test.py --duration 120
# after the burst, confirm lag drains back to baseline within seconds.
```

### 3.6 Handling 10× load

- **Shard the writer:** partition by `_id` hash into N writer threads/processes, each
  owning a disjoint key space (preserves per-key ordering, parallelises Mongo writes).
- **Parallelise per table** with FK-safe ordering preserved per partition.
- **External durable queue** (Kafka/Redis Streams) to decouple reader and writers and
  let writers scale horizontally without re-reading WAL on writer restart.
- **Mongo side:** shard the collections; raise pool size; keep `w=1` for ingest.
- The single-LSN-ordered stream becomes the scaling constraint; partitioned consumers
  per replication slot or per-table publications address it.

## 7.4 Tuning guide

| Parameter (env) | Default | Observed effect |
|---|---|---|
| `WRITE_BATCH_SIZE` | 1000 | Biggest writer lever. ↑ batch ⇒ fewer Mongo round-trips ⇒ ↑ throughput, ↑ per-batch latency. At 1000 the writer sustained ≳15k changes/s. |
| `WRITE_BATCH_LINGER` (s) | 0.5 | Max wait to fill a batch. ↓ ⇒ lower latency at low load (idle lag ~0); ↑ ⇒ fuller batches under load. Floor on lag at very low traffic. |
| `QUEUE_MAXSIZE` | 50000 | Back-pressure depth. Pinned at 50000 under overload, which **bounded `lag_seconds` to ~0.66s** (queue caps the event-time backlog). ↑ absorbs longer bursts (more RAM). **Co-tune with `WRITE_BATCH_SIZE`.** |
| `WRITE_CONCERN` (`w`) | 1 | `1` = ack from primary (fast) — used for all measurements. `majority` = durable but slower; raises lag materially under load. |
| `MONGO_POOL_SIZE` | 50 | Concurrent Mongo connections. Headroom for a sharded writer; single-writer rarely exhausts it. |
| `WRITE_RETRY_LIMIT` | 5 | Bounded retry+backoff on transient Mongo errors before surfacing. 0 errors seen across all load runs. |
| `STANDBY_MESSAGE_TIMEOUT` (s) | 5 | Feedback/keepalive cadence to the slot. ↓ advances `confirmed_flush_lsn` more often (less WAL retained), more chatter. |
| `SNAPSHOT_CHUNK_SIZE` | 5000 | Snapshot COPY/upsert batch. At 5000, snapshot of 2.25M rows took ~35s. ↑ faster, more memory per chunk. |
| `SCHEMA_CACHE_TTL` (s) | 5 | How quickly the live pipeline notices an operator `schema-sync`. T7→T8 propagation observed within this window. |
| `LAG_SLO_SECONDS` | 5 | The SLO asserted by T9. Measured max under 2× load: 0.51s. |

**Co-tuning note.** `QUEUE_MAXSIZE` and `WRITE_BATCH_SIZE` interact: the queue must
hold at least a few batches' worth of ops for the writer to stay fully fed; sizing the
queue far larger than the writer can drain only converts the burst into latency.
`REPLICA IDENTITY FULL` (schema choice) ↑ WAL volume — co-factor for reader-side cost.

## 7.5 Failure modes

1. **PostgreSQL restart / WAL slot invalidation.** The slot is persistent and survives
   a PG restart; on reconnect we resume from `confirmed_flush_lsn`. If the slot is
   *dropped* (manually, or invalidated by `max_slot_wal_keep_size`), it can no longer
   resume — `slot_exists()` is false, so on next start the pipeline re-creates the slot
   and redoes a consistent snapshot. Detected and surfaced in logs; never a silent gap.

2. **MongoDB write failure (timeout / transient).** `_bulk_write_with_retry` retries
   the whole batch with exponential backoff up to `WRITE_RETRY_LIMIT`. Because the LSN
   is confirmed only after a successful write, a failure simply stalls the offset — on
   recovery the same batch is re-applied idempotently. Duplicate-key on `_id` cannot
   occur (we upsert by `_id`).

3. **Pipeline crash mid-batch.** Events written but not yet LSN-confirmed are replayed
   from the slot after restart. The upsert + LSN guard make replays idempotent, so the
   result is effectively-once. No duplicates, no loss (validated by T6).

4. **WAL slot bloat (disk).** A slow/stuck writer holds back `confirmed_flush_lsn`, and
   Postgres retains all WAL since that LSN — unbounded disk growth is the real danger of
   logical slots. *Observed and fixed during testing:* because we initially only
   confirmed LSNs for our own tables' changes, the slot retained WAL that logical
   decoding had filtered out, leaving **~127 MB** pinned while idle. The reader now
   also confirms up to the end of received WAL once the queue is fully drained
   (`Reader._confirm`), which dropped retained WAL to **~6.7 KB**. Further mitigations:
   the bounded queue caps how far behind we fall, `lag_bytes` is exposed for alerting,
   and in production `max_slot_wal_keep_size` caps retention (trading slot invalidation
   — case 1 — for disk safety).

5. **Schema change not yet synced.** Rows inserted after `ALTER TABLE ADD COLUMN` are
   still replicated for their *tracked* columns; the new column is skipped and a
   throttled warning is logged. No crash. After `schema-sync`, existing docs are
   backfilled (null) and the column becomes tracked; the live pipeline picks it up
   within `SCHEMA_CACHE_TTL` (validated by T7/T8).

## 7.6 Trade-offs & what I'd do differently

**Why this approach.** Logical replication is the only option that is true CDC with a
durable, built-in offset and zero added latency to source writes — directly serving
"never lose events, never re-sync." `wal2json` keeps decoding in easy-to-parse JSON so
the transform layer stays small and unit-testable.

**Known gaps / shortcuts (time-boxed):**
- Single-process reader+writer. Throughput is writer-bound; sharded writers (§3.6) are
  designed but not implemented.
- Transport is an in-process queue. It is sufficient because the slot is the durability
  anchor, but a writer crash re-reads from the last confirmed LSN rather than from a
  decoupled buffer. A durable queue would shrink that replay window.
- `REPLICA IDENTITY FULL` is chosen for exact field parity (T5) and reliable
  delete/update keys, at the cost of higher WAL volume. A production system might use
  `DEFAULT` (PK only) plus a lookup for parity-critical paths.
- `lag_seconds` is the reader→writer event-time gap, which is bounded by the queue
  depth; under sustained overload additional staleness lives in the WAL backlog and is
  visible via `lag_bytes`. A single end-to-end "PG commit → Mongo apply" gauge would be
  a cleaner single number (T9 measures exactly this with sentinels).

**Production-grade version.** Partitioned/sharded writers behind Kafka; multiple slots
or per-table publications; `max_slot_wal_keep_size` with alerting on slot age and
`lag_bytes`; dead-letter handling for poison records; schema registry with versioned,
auditable sync approvals; Prometheus + Grafana instead of the plaintext endpoint; and
automated snapshot resumption (chunked, checkpointed) so a mid-snapshot crash doesn't
restart the whole snapshot.
