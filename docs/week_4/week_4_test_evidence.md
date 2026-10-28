# Bằng chứng Kiểm thử và Nghiệm thu Tuần 4 (FraudStream)

> Deliverables: W3 Platform Handoff Evidence & W4 Batch Verification Evidence  
> Contributors: Nguyễn Minh Hoàng, Lê Xuân Nhật Khôi  
> Thời gian ghi nhận: 2026-10-29

---

## 1. Nghiệm thu Hạ tầng Nền tảng Tuần 3 (Platform Handoff)

### 1.1. Kết quả Xác minh Toàn diện Hạ tầng (`verify_infra`)

Toàn bộ 4 thành phần nền tảng đều đạt trạng thái `PASS`:

```text
==================================================================
 FraudStream Infrastructure Verification (verify_infra.ps1)
 Namespace: fraudstream
==================================================================

--- [1/4] Platform & Kubernetes Cluster ---
 [PASS] Kubernetes cluster nodes are in Ready state.
 [PASS] Namespace 'fraudstream' exists.
 [PASS] CoreDNS service is Running.
 [PASS] PVC status healthy (4 Bound PVCs).

--- [2/4] Apache Cassandra Serving Store ---
 [PASS] Cassandra pod cassandra-0 is Running.
 [PASS] Cassandra node state is UN (Up Normal).
 [PASS] Keyspace 'fraudstream' exists and is accessible.
 [PASS] CQL query execution passed without ALLOW FILTERING.

--- [3/4] Apache Kafka Event Stream ---
Context: kind-fraudstream-cluster; endpoint: kafka.fraudstream.svc.cluster.local:9092
PASS: Kafka pod kafka-0 is Running and Ready.
PASS: Kafka PVC kafka-data-kafka-0 is Bound.
PASS: transactions_raw: partitions=3, replication=1, retention.ms=604800000.
PASS: transactions_invalid: partitions=1, replication=1, retention.ms=259200000.
PASS: fraud_alerts: partitions=3, replication=1, retention.ms=604800000.
PASS: fraud_feedback: partitions=1, replication=1, retention.ms=2592000000.
PASS: Kafka health and all 4 topic configurations verified.
[1/4] Creating a temporary Kafka client pod...
[2/4] Sending 100 normal simulator events...
[3/4] Reading new records and checking JSON, schema and Kafka keys...
PASS: 100/100 events received; JSON, schema and Kafka keys match.
 [PASS] Kafka pod/PVC/topics and 100-event producer-consumer test passed.

--- [4/4] Apache HDFS Data Lake ---
 [PASS] HDFS NameNode pod hdfs-namenode-0 is Running.
 [PASS] HDFS path exists: /fraud-data/raw
 [PASS] HDFS path exists: /fraud-data/cleaned
 [PASS] HDFS path exists: /fraud-data/curated
 [PASS] HDFS path exists: /fraud-data/late_events
 [PASS] HDFS path exists: /fraud-data/checkpoints
 [PASS] HDFS path exists: /fraud-data/models
 [PASS] HDFS DataNode pod hdfs-datanode-0 is Running.
 [PASS] HDFS PVC hdfs-namenode-data-hdfs-namenode-0 is Bound.
 [PASS] HDFS PVC hdfs-datanode-data-hdfs-datanode-0 is Bound.
 [PASS] HDFS PowerShell I/O read/write verification passed.

==================================================================
 Infrastructure Verification Summary
==================================================================
 STATUS: ALL VERIFIED CHECKS PASSED
==================================================================
```

---

## 2. Nghiệm thu Nửa đầu Batch Data Pipeline Tuần 4

### 2.1. Quá trình Submit Batch Job (`submit_batch`)
Job thực thi với fixture v1 gồm 4 bản ghi:

```text
============================================================
 Submitting FraudStream Batch Job: batch_fixture_v1
 Input HDFS: /fraud-data/raw/transactions_raw/ingest_date=2026-10-04/batch_id=batch_fixture_v1/transactions.jsonl
 Ref Time  : 2026-10-04T10:00:00Z
============================================================
[1/4] Launching batch runner pod batch-batch-fixture-v1-runner in namespace fraudstream...
[2/4] Setting up batch code and reading raw data from HDFS...
[3/4] Executing batch processing transformations...
PASS: Batch batch_fixture_v1 committed successfully. Run ID: fb18c012da74
Metrics: {"input_records": 4, "parsed_records": 4, "parse_invalid": 0, "valid_records": 1, "invalid_records": 1, "duplicate_records": 1, "late_records": 1, "cleaned_records": 1, "curated_records": 1, "distinct_transaction_ids": 1, "classification_reconciliation": true}
[4/4] Committing cleaned output and manifest back to HDFS...

PASS: Batch Job batch_fixture_v1 completed successfully in terminal state: Succeeded
```

### 2.2. Kết quả Chạy Verifier (`verify_batch`)

```text
==================================================================
 FraudStream Batch Verification (verify_batch.py)
 Target Batch ID: batch_fixture_v1
 Namespace      : fraudstream
==================================================================

--- [1/6] Run Manifest Validation ---
 [PASS] Manifest exists at /fraud-data/manifests/batch_id=batch_fixture_v1/run_manifest.json
 [PASS] Manifest status is COMMITTED.
 [PASS] Schema version matches 'transaction_event_v1'.

--- [2/6] Raw Data Immutability & Checksum ---
 [PASS] Raw data SHA-256 matches manifest checksum: 523bb46244...
 [PASS] Raw line count (4) matches manifest input_records.
 [PASS] Raw HDFS content is byte-identical to original versioned fixture.

--- [3/6] Cleaned Output & Date Partitioning ---
 [PASS] Cleaned partition exists: /fraud-data/cleaned/event_date=2026-10-04 (1 file(s))

--- [4/6] Schema Compliance & Uniqueness ---
 [PASS] All 1 cleaned records strictly adhere to 12 fields + event_date.
 [PASS] All 1 cleaned records have unique transaction_id (no duplicates).

--- [5/6] Metric Reconciliation ---
       input=4, valid=1, invalid=1, duplicate=1, late=1, cleaned=1
 [PASS] Reconciliation invariant holds: input (4) == valid (1) + invalid (1) + dup (1) + late (1)
 [PASS] Cleaned row count (1) equals valid count (1).
 [PASS] Fixture counts exactly match v1 contract expectations (4 -> 1,1,1,1 -> cleaned 1).

--- [6/6] Idempotency & Failure Semantics ---
 [PASS] Batch committed with deterministic run_id: fb18c012da74
 [PASS] Idempotency check: repeat read yields identical checksum and record counts.

==================================================================
 STATUS: BATCH VERIFICATION PASSED (ALL CHECKS PASS)
==================================================================
```

---

## 3. Bảng Đối soát Số liệu (Metric Reconciliation)

| Chỉ số | Giá trị | Đối chiếu / Kiểm chứng | Kết luận |
|---|---:|---|---|
| `input_records` | 4 | Số dòng file JSONL raw trên HDFS | Khớp line count gốc |
| `valid_records` | 1 | Bản ghi `tx_fixture_valid_001` (dòng 1) | Ghi vào cleaned Parquet |
| `invalid_records` | 1 | Bản ghi `tx_fixture_invalid_001` (dòng 2: `amount = -10.0`) | Cách ly tại `raw/invalid/` |
| `duplicate_records` | 1 | Bản ghi `tx_fixture_valid_001` lặp lại (dòng 3) | Loại bỏ, tăng metric duplicate |
| `late_records` | 1 | Bản ghi `tx_fixture_late_001` (dòng 4: trễ 20 phút so với max) | Lưu tại `late_events/` |
| `cleaned_records` | 1 | Số bản ghi ghi nhận tại `/fraud-data/cleaned/` | Khớp `valid_records` |
| `distinct_tx_ids` | 1 | Số transaction ID duy nhất trong cleaned | Không có duplicate trong output |
