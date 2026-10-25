# Handoff Contract: Kafka, HDFS và Batch Pipeline v1

## 1. Mục đích

Tài liệu chốt giao diện giữa simulator/Kafka, HDFS raw storage và batch
pipeline. Nó không khẳng định Spark job đã được triển khai; Spark sẽ dùng
contract này ở giai đoạn batch.

## 2. Luồng và địa chỉ

```text
Simulator -> transactions_raw (Kafka) -> /fraud-data/raw/*.jsonl (HDFS)
          -> validation/dedup/late routing -> cleaned/curated Parquet (batch)
```

| Hạng mục | Contract |
|---|---|
| Kafka topic đầu vào | `transactions_raw` |
| Kafka bootstrap nội bộ | `kafka.fraudstream.svc.cluster.local:9092` |
| Kafka key | `card_id_hash`, UTF-8 |
| Kafka value | JSON UTF-8 theo `transaction_event_v1` |
| Idempotency key | `transaction_id` |
| Raw source | JSONL immutable trong `/fraud-data/raw/` |
| Invalid quarantine | `/fraud-data/raw/invalid/` (đề xuất v1; không ghi vào cleaned) |
| Late events | `/fraud-data/late_events/` |
| Cleaned | `/fraud-data/cleaned/`, Parquet + Snappy, partition `event_date=YYYY-MM-DD` |
| Curated | `/fraud-data/curated/`, Parquet + Snappy, cùng partition v1 |
| Spark checkpoint | `/fraud-data/checkpoints/` |
| Model artifacts | `/fraud-data/models/` |

## 3. Event-time, data quality và routing

- `event_time` là ISO-8601 UTC; `event_date` lấy từ UTC date của event time.
- Watermark khởi điểm là 10 phút.
- Thứ tự phân loại không thay đổi: `invalid -> duplicate -> late -> valid`.
- Invalid: ghi reason code và quarantined raw record; không vào cleaned/curated.
- Duplicate: nhận diện bằng `transaction_id`, loại khỏi downstream, tăng metric.
- Late: ghi vào `late_events`, phục vụ controlled batch backfill; không đổi raw
  history âm thầm.
- Valid: đi vào cleaned và curated. Curated v1 bằng cleaned cho đến khi feature
  enrichment được hiện thực và kiểm tra leakage.

## 4. Tái lập integration smoke flow

Sau khi cluster, Kafka và HDFS đều Ready:

```bash
bash infra/hdfs/setup_datalake.sh
python3 infra/scripts/integration_smoke.py
```

Script sinh 100 normal simulator events với seed cố định, gửi/đọc chúng qua
Kafka với key `card_id_hash`, ghi chính JSONL đó vào HDFS raw và so checksum
SHA-256. Nó để file raw đã kiểm tra trong HDFS làm evidence, nhưng xóa client
pod và file tạm trên host.

## 5. Giới hạn hiện tại

Spark chưa ghi Parquet, chưa tính feature hoặc score. Integration smoke chỉ
chứng minh input contract từ simulator qua Kafka và raw HDFS; không thay thế
cho batch/streaming runtime test.
