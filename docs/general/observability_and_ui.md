# Observability and UI Verification

## Components

| Component | Endpoint / role | Verification |
|---|---|---|
| Flask dashboard | `/`, `/api/summary`, `/api/decisions` | Flask test client checks HTML and JSON response |
| Scoring worker exporter | `fraudstream-scoring:8000/metrics` | Counters for route, decision and scoring latency |
| Prometheus | `prometheus.fraudstream.svc:9090` | Scrapes the deployed dashboard exporter every 15 seconds |
| Grafana | `grafana.fraudstream.svc:3000` | Provisioned Prometheus datasource and FraudStream overview dashboard |

## Metrics

- `fraudstream_events_total{route}`: valid/scored, invalid, duplicate and late records.
- `fraudstream_decisions_total{decision}`: APPROVE, REVIEW, BLOCK decisions.
- `fraudstream_event_processing_seconds`: online scoring latency distribution for p50/p95 queries.
- `fraudstream_kafka_consumer_lag`: current scoring consumer lag.
- `fraudstream_spark_input_rows_per_second`, `fraudstream_spark_processed_rows_per_second`,
  `fraudstream_spark_batch_duration_ms`, `fraudstream_spark_state_rows`: Structured Streaming runtime metrics.
- `kube_pod_status_phase` and `kube_pod_container_status_restarts_total`: Kubernetes workload health from kube-state-metrics.

## Commands

```bash
make monitoring  # build/load UI + scoring images, then deploy/restart monitoring services
kubectl -n fraudstream port-forward svc/prometheus 9090:9090
kubectl -n fraudstream port-forward svc/grafana 3000:3000
```

Run the complete local regression suite with `python3 -m pytest tests -q`.
The UI/metrics unit test creates velocity traffic, asserts an alert, renders the
UI, reads summary JSON and asserts that Prometheus metric exposition contains
the event counter. For a cluster integration test, emit a velocity scenario to
Kafka and query `fraudstream_decisions_total` in Prometheus; the scoring worker
is the process that owns those metrics.

## Runtime evidence

Latest local integration evidence: both Prometheus targets were healthy; after
a five-event velocity sequence, Prometheus reported `APPROVE=104`, `REVIEW=1`,
and the Cassandra-backed dashboard rendered the `REVIEW` record with score
`0.60` and reason `velocity: tx_count_5m >= 5`.

The monitoring stack provisions the datasource and the FraudStream overview
dashboard as part of the reproducible Kubernetes deployment. The provisioned
dashboard has panels for throughput by route, fraud decisions, p95 scoring
latency, Kafka consumer lag, Spark input/processed rate, Spark batch duration,
Spark state rows, invalid/duplicate/late quality routes, running pod count and
container restart count.
