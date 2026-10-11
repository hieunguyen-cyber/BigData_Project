# Data Dictionary và data-quality contract

## 1. Transaction event v1

Mỗi record trong `transactions_raw` là một JSON object UTF-8. `card_id_hash` là Kafka message key; schema giữ thống nhất cho simulator, Spark và các sink.

```json
{
  "transaction_id": "tx_00000001",
  "event_time": "2026-10-04T09:00:00Z",
  "customer_id": "cus_normal-card-00000",
  "card_id_hash": "sha256:<64 lowercase hex characters>",
  "merchant_id": "merchant_0001",
  "amount": 180000.0,
  "currency": "VND",
  "channel": "web",
  "device_id_hash": "sha256:<64 lowercase hex characters>",
  "ip_hash": "sha256:<64 lowercase hex characters>",
  "country": "VN",
  "transaction_status": "success"
}
```

| Field | Kiểu | Bắt buộc | Ràng buộc | Ý nghĩa |
|---|---|---:|---|---|
| `transaction_id` | string | Có | Duy nhất trong stream | Idempotency/deduplication key |
| `event_time` | ISO-8601 UTC string | Có | Có timezone, dạng `...Z` | Thời điểm nghiệp vụ dùng cho windows/watermark |
| `customer_id` | string | Có | Synthetic identifier | Khách hàng mô phỏng |
| `card_id_hash` | string | Có | `sha256:` + 64 lowercase hex | Key của card behaviour/Kafka partition |
| `merchant_id` | string | Có | Synthetic identifier | Merchant thực hiện giao dịch |
| `amount` | decimal/float | Có | `> 0` | Giá trị giao dịch VND |
| `currency` | string | Có | ISO 4217, 3 chữ in hoa | Đơn vị tiền tệ |
| `channel` | string | Có | `web`, `mobile`, hoặc `api` | Kênh thanh toán |
| `device_id_hash` | string | Có | SHA-256 synthetic hash | Thiết bị phát sinh giao dịch |
| `ip_hash` | string | Có | SHA-256 synthetic hash | IP đã hash |
| `country` | string | Có | ISO 3166-1 alpha-2 | Quốc gia giao dịch |
| `transaction_status` | string | Có | `success` hoặc `failed` | Trạng thái xử lý giao dịch |

## 2. Quy tắc phân loại dữ liệu

Thứ tự kiểm tra là **invalid -> duplicate -> late -> valid**. Mỗi event chỉ được gán một trạng thái.

| Rule | Điều kiện | Phân loại | Xử lý |
|---|---|---|---|
| DQ-01 | Thiếu trường; `channel` ngoài tập cho phép | invalid | Ghi `transactions_invalid` kèm lý do |
| DQ-02 | `amount` không phải số hoặc `amount <= 0` | invalid | Ghi `transactions_invalid` |
| DQ-03 | `transaction_status` khác `success`/`failed` | invalid | Ghi `transactions_invalid` |
| DQ-04 | `event_time` sai ISO-8601 UTC | invalid | Ghi `transactions_invalid` |
| DQ-05 | `event_time` muộn hơn processing time trên 5 phút | invalid | Ghi `transactions_invalid` |
| DQ-06 | Currency/country không đúng định dạng | invalid | Ghi `transactions_invalid` |
| DQ-07 | Amount cực lớn | valid + `extreme_amount` flag | Không loại vì có thể fraud |
| DQ-08 | `transaction_id` đã xử lý | duplicate | Bỏ downstream, tăng metric |
| DQ-09 | Event time trễ quá `max_event_time - 10 phút` | late | Ghi `late_events` để batch backfill |
| DQ-10 | ID nhạy cảm không phải SHA-256 synthetic hash | invalid | Ghi `transactions_invalid` |

## 3. Topics và trách nhiệm

| Topic | Kafka key | Producer | Consumer | Nội dung |
|---|---|---|---|---|
| `transactions_raw` | `card_id_hash` | Simulator/payment source | Spark streaming | Transaction hợp schema |
| `transactions_invalid` | `transaction_id` | Spark validation | Quality dashboard | Record sai data contract |
| `fraud_alerts` | `card_id_hash` | Spark scoring | Alert dashboard/service | Quyết định REVIEW/BLOCK |
| `fraud_feedback` | `transaction_id` | Human review/simulator | Batch ML job | Nhãn xác nhận về sau |

## 4. Kiểm thử contract

```bash
# Sinh normal stream và quality cases có chủ đích.
python data-generator/producer.py --scenario normal --seed 42 --rate 20 --duration 60 \
  --output out/normal.jsonl --quality-output out/quality_cases.jsonl

# Chạy toàn bộ unit/integration tests của Tuần 1.
python -m pytest tests -q
```

`out/quality_cases.jsonl` lần lượt có một event `valid`, `invalid`, `duplicate`, `late`; chi tiết implementation nằm trong `data-generator/schema.py`.
