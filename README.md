# Custom CDC Pipeline — PostgreSQL → MongoDB

A from-scratch Change Data Capture pipeline that streams row-level changes
(INSERT / UPDATE / DELETE) from PostgreSQL into MongoDB using **PostgreSQL logical
replication** (the `wal2json` output plugin). It performs a consistent initial
snapshot, then streams continuously, surviving high write load by lagging rather than
dropping events.

See [DESIGN.md](DESIGN.md) for architecture, resilience analysis, tuning, and failure
modes.

## Stack at a glance

| Concern | Choice |
|---|---|
| Capture | Logical decoding slot + `wal2json` (true CDC; offset = slot LSN) |
| Transport | In-process bounded queue (back-pressure; slot is the durable offset) |
| Writer | Batched, idempotent, LSN-guarded `bulk_write` upserts |
| Deletes | Soft-delete (`deleted_at` set; document retained) |
| Source schema | `orders` (parent) + `order_items` (child, FK) — e-commerce |

## Prerequisites

- Docker + Docker Compose (brings up everything).
- To run the seed / load / correctness scripts **from the host**, you also need
  Python 3.11+ with `pip install -r requirements.txt`. They read `PG_DSN` / `MONGO_URI`
  from the environment and default to `localhost` (the compose file publishes
  Postgres on `5432` and Mongo on `27017`).

## 1. Bring the stack up (single command)

```bash
docker compose up --build
```

This starts Postgres (with `wal2json` and the schema from `sql/schema.sql`), MongoDB,
and the pipeline. On first start the pipeline creates the replication slot, runs the
consistent snapshot, then switches to streaming. Snapshot progress is logged:

```
cdc.snapshot snapshot orders: 50000 rows
cdc.reader   streaming from slot cdc_slot
```

## 2. Seed the source (≥ 500k rows, one command)

In another terminal:

```bash
docker compose exec pipeline python scripts/seed.py            # 500,000 orders + ~2-3 items each
# or a custom size:
docker compose exec pipeline python scripts/seed.py --orders 500000
```

> Order of operations: if you seed **before** the snapshot has run, those rows are
> captured by the snapshot. If you seed **after**, they flow in via streaming. Both are
> correct. For a clean "snapshot of 500k" demo, seed first, then `docker compose up`.

## 3. Run the load simulation

```bash
docker compose exec pipeline python scripts/load_test.py --duration 120
```

Drives INSERT ≈4000/s, UPDATE ≈1500/s, DELETE ≈350/s and prints a summary
(total ops, ops/sec, errors). Rates are tunable: `--insert-rate`, `--update-rate`,
`--delete-rate`.

## 4. Run the correctness suite (T1–T10)

Run **from the host** (T6 restarts the pipeline container, T8/T9 shell out to the
pipeline CLI and load script):

```bash
pip install -r requirements.txt
python tests/run_correctness.py
```

Each test prints `PASS`/`FAIL` with a reason and ends with `X/10 tests passed`.
Pure-logic unit tests (no DB) run with:

```bash
pytest tests/test_transform.py
```

## 5. Observe replication lag

The pipeline logs a metrics line every few seconds and serves a plaintext endpoint:

```bash
curl localhost:8000        # cdc_lag_seconds, cdc_lag_bytes, cdc_read_rate, cdc_write_rate, ...
```

## 6. Schema change propagation

New PG columns do **not** appear in Mongo until an operator triggers a sync (Airbyte
style). To propagate:

```bash
# add a column in PG (pipeline keeps running, logs a drift warning, skips the column)
docker compose exec postgres psql -U cdc -d shop -c "ALTER TABLE orders ADD COLUMN promo_code text;"

# explicitly sync: registers the column and backfills existing Mongo docs with null
docker compose exec pipeline python -m pipeline.main schema-sync orders
```

## CLI reference

```bash
python -m pipeline.main run                 # snapshot (first run) then stream
python -m pipeline.main schema-sync [table] # align Mongo schema with PG, backfill
python -m pipeline.main status              # one-shot offset + lag snapshot
```

## Repository layout

```
docker-compose.yml      Dockerfile.postgres   Dockerfile.pipeline
sql/schema.sql          orders + order_items, enum, REPLICA IDENTITY FULL
pipeline/               config, db, transform, snapshot, reader, writer,
                        schema_registry, offset, metrics, main
scripts/                seed.py, load_test.py
tests/                  run_correctness.py (T1-T10), test_transform.py, helpers.py
```
