# Nghiệm thu cuối FraudStream

## Mục đích

Tài liệu này là checklist nghiệm thu duy nhất cho các yêu cầu kỹ thuật ở
README mục 10–15. Mỗi hàng liên kết requirement với code, lệnh tái lập và tín
hiệu PASS cần quan sát. Đợt nghiệm thu lại ngày 03/11/2026 đã chạy unit,
Spark batch/ML/optimization, benchmark, rollout dashboard và kiểm tra trực tiếp
API/Prometheus. Chỉ đánh dấu đạt khi lệnh tương ứng trả exit code 0.

## 1. Checklist kiến trúc Lambda

| Requirement | Thành phần hiện thực | Lệnh nghiệm thu | PASS cần quan sát |
|---|---|---|---|
| Ingestion Kafka | simulator, topics, producer/consumer smoke | `make reproduce` | 100 event Kafka → HDFS, SHA-256 khớp |
| HDFS data lake | raw, cleaned, curated, late, checkpoints, models | `make verify && make persistence` | DataNode live, HDFS read/write và recreate persistence PASS |
| Cassandra serving | `transaction_risk`, `fraud_alert`, `dashboard_recent_decision` | `bash infra/cassandra/smoke_test.sh` | schema, insert/select và persistence PASS |
| Batch Spark | `spark-batch/spark_job.py` | `make spark-test` | cleaned + curated Snappy Parquet, manifest `4/1/1/1/1` |
| MLlib | `ml/spark_mllib_train.py` | `make spark-ml-test` | model artifact, validation/test metric JSON |
| Speed layer | `streaming/spark_structured_job.py` | Structured Kafka smoke | Kafka input, state/watermark/checkpoint, part file + `_SUCCESS` |
| Serving/UI | `dashboard/`, Cassandra read model | `make monitoring`, mở dashboard | summary/API/table hiện decision/alert Cassandra |
| Observability | Prometheus, Grafana, kube-state-metrics | `make monitoring`, `/targets` | dashboard/scoring/kube-state targets healthy |

## 2. Checklist Spark theo README

| Requirement | Bằng chứng code | Lệnh | PASS |
|---|---|---|---|
| Window + complex aggregation | `spark-batch/spark_job.py` feature windows | `make spark-test` | curated feature contract `feature_v1` |
| Transformations, UDF, business rules | `spark-batch/optimization_job.py`, `streaming/engine.py` | `make spark-optimization-test` | `udf_applied=true` |
| Broadcast join | optimization job merchant profile join | `make spark-optimization-test` | `broadcast_join_present=true` |
| Sort-merge join | optimization job customer profile join | `make spark-optimization-test` | `sort_merge_join_present=true` |
| Pruning/cache/persist/explain | optimization job + `out/spark-optimization/execution_plans.json` | `make spark-optimization-test` | rows pruned=1, physical plan saved |
| Output mode/watermark/state/checkpoint | `spark_structured_job.py` | Structured Kafka smoke | append query, checkpoint dir, Spark state metrics |
| Supervised MLlib | LR/RF training and model selection | `make spark-ml-test` | `metrics.json` + Spark model directory |

## 3. Monitoring checklist

| Nhóm metric | PromQL / source |
|---|---|
| Rate, quality, decisions, latency | `fraudstream_events_total`, `fraudstream_decisions_total`, `fraudstream_event_processing_seconds` |
| Kafka lag | `fraudstream_kafka_consumer_lag` |
| Spark runtime | `fraudstream_spark_input_rows_per_second`, `fraudstream_spark_processed_rows_per_second`, `fraudstream_spark_batch_duration_ms`, `fraudstream_spark_state_rows` |
| Kubernetes health | `kube_pod_status_phase`, `kube_pod_container_status_restarts_total` |

## 4. Full final command order

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH=/opt/homebrew/opt/openjdk@17/bin:$PATH

make reproduce
make persistence
python3 -m pytest tests -q
make spark-test
make spark-optimization-test
make spark-ml-test
make benchmark
make monitoring
```

Sau đó mở dashboard, Prometheus và Grafana bằng port-forward:

```bash
kubectl -n fraudstream port-forward svc/fraudstream-dashboard 8080:8080
kubectl -n fraudstream port-forward svc/prometheus 9090:9090
kubectl -n fraudstream port-forward svc/grafana 3000:3000
```

## 5. Artefact nộp và demo

- Báo cáo kiến trúc/evidence: `docs/checkpoints/final_system_report.md`.
- Kịch bản demo: README mục 15 và runbook ở trên.
- Slides checkpoint: `slides/checkpoint_w1_w3/`.
- Code/test/hạ tầng: repository root, `tests/`, `infra/`, `spark-batch/`,
  `streaming/`, `ml/`, `dashboard/`.

## 6. Biên bản nghiệm thu lại (03/11/2026)

| Hạng mục | Kết quả quan sát được | Kết luận |
|---|---|---|
| Unit + API contract | `35 passed` | Đạt |
| Spark batch | Parquet được tạo; manifest `input=4`, `valid=1`, `invalid=1`, `duplicate=1`, `late=1`, `curated=1` | Đạt |
| Spark MLlib | Logistic Regression được chọn; validation PR-AUC `0.7707`, test PR-AUC `0.8445` | Đạt |
| Spark optimization | UDF, broadcast join, sort-merge join, cache và partition pruning đều `true` | Đạt |
| Streaming engine benchmark | 1,000 events; `121,130.75 events/s`, p50 `0.0069 ms`, p95 `0.0088 ms` | Đạt |
| Kubernetes | Cassandra, Kafka, HDFS NameNode/DataNode, dashboard, scoring, Prometheus, Grafana và kube-state-metrics đều `Running`/`Ready` | Đạt |
| Dashboard serving | `GET /healthz` trả `{"status":"ok"}`; `GET /api/summary` trả `total=50`; pod mới `restart=0` | Đạt |
| Monitoring | Prometheus targets `fraudstream-dashboard`, `fraudstream-scoring`, `kube-state-metrics` đều `up` | Đạt |

Dashboard dùng endpoint `/healthz` độc lập với truy vấn Cassandra cho liveness
và readiness; nhờ đó một truy vấn read model chậm không thể làm Kubernetes
restart service. Server được chạy threaded để việc đọc feed không chặn probe.
