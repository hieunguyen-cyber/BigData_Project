# Runbook demo cuối — FraudStream

Tài liệu này là kịch bản nghiệm thu và quay demo cuối của FraudStream. Mỗi bước
chỉ được đánh dấu xong khi command có exit code `0` và kết quả quan sát khớp
với cột **Bằng chứng cần thấy**.

## 1. Chuẩn bị môi trường

```bash
cd BigData_Project
python3 -m pip install -r requirements.txt
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH=/opt/homebrew/opt/openjdk@17/bin:$PATH
```

Mở ba terminal riêng cho dashboard, Prometheus và Grafana sau khi monitoring
đã sẵn sàng:

```bash
kubectl -n fraudstream port-forward svc/fraudstream-dashboard 8080:8080
kubectl -n fraudstream port-forward svc/prometheus 9090:9090
kubectl -n fraudstream port-forward svc/grafana 3000:3000
```

Các URL demo:

- UI vận hành: `http://localhost:8080`
- Prometheus targets: `http://localhost:9090/targets`
- Grafana: `http://localhost:3000/d/fraudstream-overview/fraudstream-overview`

## 2. Checklist nghiệm thu chạy trước khi quay

| # | Lệnh | Bằng chứng cần thấy | Thời lượng dự kiến |
|---:|---|---|---:|
| 1 | `make reproduce` | Kind, Kafka, HDFS, Cassandra healthy; simulator → Kafka → HDFS PASS | 5–8 phút |
| 2 | `make persistence` | Kafka, Cassandra, HDFS persistence PASS | 2–4 phút |
| 3 | `make test` | Toàn bộ Python tests pass | < 1 phút |
| 4 | `make spark-test` | `cleaned_records=1`, `curated_records=1`, Parquet write PASS | < 1 phút |
| 5 | `make spark-optimization-test` | `BroadcastHashJoin=true`, `SortMergeJoin=true`, `cache_persisted=true`, `udf_applied=true` | < 1 phút |
| 6 | `make spark-ml-test` | Spark MLlib chọn model và in PR-AUC/ROC-AUC/F1 | < 1 phút |
| 7 | `make benchmark` | throughput, p50, p95 được ghi ở `out/benchmarks/streaming.json` | < 1 phút |
| 8 | `make monitoring` | dashboard, scoring, Prometheus, Grafana, kube-state-metrics rollout success | 2–4 phút |
| 9 | Prometheus `/targets` | `fraudstream-dashboard`, `fraudstream-scoring`, `kube-state-metrics` đều `UP` | 15 giây |

## 3. Kịch bản quay video 10–12 phút

### Phần A — Bài toán và kiến trúc (0:00–1:30)

Mở `README.md`, [bài toán và kiến trúc](../general/problem_and_architecture.md)
và slide `slides/checkpoint_w1_w3/main.pdf`. Trình bày ngắn:

1. FraudStream nhận event thanh toán online, trả `APPROVE`, `REVIEW`, `BLOCK`.
2. Kafka là lớp ingestion; HDFS là data lake; Cassandra là serving store.
3. Lambda path gồm batch (Spark SQL/MLlib) và speed (Structured Streaming).
4. Prometheus/Grafana/UI là lớp quan sát và vận hành.

### Phần B — Dữ liệu và chất lượng (1:30–3:00)

Mở `docs/general/data_dictionary.md` và phát một scenario:

```bash
kubectl -n fraudstream port-forward svc/kafka 19092:9094
# terminal khác:
python3 data-generator/producer.py --scenario velocity_fraud --rate 8 --duration 1 --seed 42 \
  --bootstrap-server localhost:19092 --topic transactions_raw
```

Nêu 12 field canonical, `transaction_id` idempotency key, event time UTC,
hash synthetic cho định danh. Minh họa thứ tự rule: invalid → duplicate → late
→ valid.

### Phần C — Batch và feature engineering (3:00–5:00)

Chạy:

```bash
make spark-test
make spark-optimization-test
```

Màn hình cần giữ lại phần output chứng minh: raw → cleaned → curated, Snappy
Parquet partition theo `event_date/event_hour`, event-time windows, UDF,
broadcast join, sort-merge join, cache/persist và physical plan.

### Phần D — MLlib (5:00–6:30)

```bash
make spark-ml-test
```

Show JSON output model selection và các metric PR-AUC, ROC-AUC, F1. Nêu model
artifact nằm tại `out/models/spark-mllib-v1/` và feature contract là
`feature_v1`.

### Phần E — Streaming, alert và serving (6:30–8:30)

1. Mở Kafka topic và scoring worker logs:

   ```bash
   kubectl -n fraudstream logs deployment/fraudstream-scoring --tail=30
   kubectl -n fraudstream get pods -o wide
   ```

2. Phát 5 event cùng `card_id_hash` trong cửa sổ 5 phút bằng scenario velocity.
3. Show log có `REVIEW`/`BLOCK`, feature `tx_count_5m`, trigger reason.
4. Mở dashboard `http://localhost:8080`: alert, risk score, transaction ID,
   trigger reasons phải xuất hiện.

### Phần F — Monitoring và Kubernetes (8:30–10:30)

Mở Prometheus `/targets`, sau đó Grafana overview. Các panel/metric cần show:

- throughput theo `fraudstream_events_total`;
- APPROVE/REVIEW/BLOCK;
- p95 latency;
- Kafka consumer lag;
- Spark input/processed rate, batch duration, state rows;
- `kube_pod_status_phase` và restart count của workload.

Chạy benchmark để kết luận performance local:

```bash
make benchmark
cat out/benchmarks/streaming.json
```

### Phần G — Tổng kết và tái lập (10:30–12:00)

Kết luận bằng các lệnh một người mới có thể dùng để tái lập:

```bash
make reproduce
make test
make spark-test
make spark-ml-test
make spark-optimization-test
make benchmark
make monitoring
```

## 4. Checklist file cần đưa vào bản nộp

- `README.md` và `docs/` (bao gồm báo cáo nghiệm thu và runbook này).
- `requirements.txt`, `Makefile`.
- `data-generator/`, `spark-batch/`, `streaming/`, `ml/`, `dashboard/`.
- `infra/` manifests cho Kind, Kafka, HDFS, Cassandra, scoring, Prometheus,
  Grafana, kube-state-metrics.
- `slides/checkpoint_w1_w3/main.tex` và PDF render (slide source bị ignore
  theo thiết lập repository; kiểm tra bổ sung khi tạo archive nộp bài).

## 5. Capture list cho video

1. Architecture diagram.
2. `make reproduce` PASS.
3. Batch manifest và curated Parquet output.
4. Spark optimization JSON và MLlib JSON.
5. Streaming alert trong worker log, Cassandra-backed dashboard.
6. Prometheus targets và Grafana panels.
7. `make benchmark` report.
