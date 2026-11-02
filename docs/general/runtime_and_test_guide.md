# FraudStream — Runtime, Test and Demo Guide

## Implemented end-to-end components

1. **Event contract and simulator:** canonical 12-field JSON events, deterministic
   normal/card-testing/velocity/device-sharing/high-value scenarios, quality
   routing for invalid, duplicate and late records.
2. **Batch:** `spark-batch/spark_job.py` runs a real PySpark job. It creates
   immutable batch-scoped outputs: cleaned Snappy Parquet, invalid/duplicate/late
   routes and a JSON run manifest. Batch ID reuse is rejected by the Spark job;
   the local runner is a no-op only for the same committed checksum.
3. **Online decision engine:** `streaming/engine.py` maintains event-time
   windows and emits APPROVE/REVIEW/BLOCK plus explicit trigger reasons.
   `streaming/stream_job.py` is the Kafka adapter and publishes alerts to
   `fraud_alerts`.
4. **Serving/UI:** Cassandra schema stores risk/alert records. The small Flask
   UI in `dashboard/app.py` renders decision totals and recent decisions, with
   `/api/summary` and `/api/decisions` for integration.

## Local verification

Install dependencies and use a supported JDK:

```bash
python3 -m pip install -r requirements.txt
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH=/opt/homebrew/opt/openjdk@17/bin:$PATH
python3 -m pytest tests -q
python3 spark-batch/spark_job.py \
  --input-path tests/fixtures/batch/v1/transactions.jsonl \
  --output-root /tmp/fraudstream-batch \
  --batch-id demo_001 --reference-time 2026-10-04T10:00:00Z
```

The fixture acceptance result is `input=4`, `valid=1`, `invalid=1`,
`duplicate=1`, `late=1`, `cleaned=1`. A successful Spark run writes a Snappy
Parquet file under `cleaned/batch_id=demo_001/event_date=2026-10-04/` plus
quarantine outputs and a run manifest.

## Cluster verification and streaming demo

```bash
make reproduce
make verify
python3 streaming/stream_job.py \
  --bootstrap-server kafka.fraudstream.svc.cluster.local:9092
```

The Kind cluster must be running before these commands. Kafka/HDFS/Cassandra are
local single-node demo services: they prove interfaces and persistence through
pod recreation, not HA/security guarantees.

## Test matrix

| Test family | Assertion |
|---|---|
| Schema/scenarios | contract, PII-safe hashes, five simulator scenarios |
| Batch fixture | mutually exclusive quality classification and metric reconciliation |
| Batch idempotency | same batch/checksum is a no-op; a conflicting input is rejected |
| PySpark smoke | real Spark JVM writes partitioned Parquet locally |
| Streaming/UI | velocity window raises alert; duplicate/invalid routing works; Flask API/UI respond |
| Infrastructure | `make verify`, persistence scripts and `integration_smoke.py` when Kind runs |

## Known local limits

- Spark 4.0.1 requires a supported LTS Java runtime here; OpenJDK 17 is used.
- The current Kafka adapter is a Python stateful reference implementation.
  A production deployment should port state/windowing to Spark Structured
  Streaming with checkpointed state and an exactly-once sink strategy.
- Cassandra serving writes should be wired by the streaming deploy adapter
  before claiming a live production dashboard.
