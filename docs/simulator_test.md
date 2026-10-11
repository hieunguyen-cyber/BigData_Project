# Simulator Test & Data-Quality Rules (W1-04, W1-05)

> Deliverable đồng sở hữu: **Nguyễn Trung Hiếu** (simulator, schema, rule hiện thực) và **Phạm Trung Hiếu** (rule rút ra từ EDA, review tính hợp lý dữ liệu, sample event).
> Mục có nhãn **[PTH]** do Phạm Trung Hiếu soạn; phần **[NTH]** đã được Nguyễn Trung Hiếu chốt theo simulator thực tế.

## 1. Phân loại event: valid / invalid / duplicate / late

| Loại | Định nghĩa | Hành vi mong đợi | Topic |
|---|---|---|---|
| **valid** | Đủ 12 trường bắt buộc, đúng kiểu, `amount > 0`, `transaction_status ∈ {success, failed}`, `event_time` hợp lệ, `transaction_id` chưa gặp, không trễ quá watermark | Vào pipeline | `transactions_raw` → xử lý |
| **invalid** | Vi phạm schema/constraint (xem 2) | Loại khỏi pipeline chính, ghi lý do | `transactions_invalid` |
| **duplicate** | `transaction_id` đã xử lý trước đó | Bỏ bản sau (idempotent); đếm metric | không phát lại xuống sau |
| **late** | Hợp lệ nhưng `event_time` < watermark hiện tại (`max(event_time) − độ trễ cho phép`) | Không cập nhật state online; ghi `late_events` để batch xử lý lại | `late_events` (HDFS) |

Thứ tự kiểm tra đề xuất: parse → schema/constraint → duplicate → late. Một event chỉ thuộc **một** loại theo thứ tự này.

## 2. Rule data-quality **[PTH]** - rút ra từ EDA

| Mã | Rule | Loại khi vi phạm | Cơ sở |
|---|---|---|---|
| DQ-01 | Thiếu bất kỳ trường bắt buộc (null/rỗng) | invalid | Data contract |
| DQ-02 | `amount` không phải số hoặc `amount ≤ 0` | invalid | EDA ULB ghi nhận số giao dịch `Amount = 0` (xem `eda_report.md` mục 5) → nhóm cần **quyết định**: coi `amount = 0` là invalid (đề xuất), hay valid + cờ `zero_amount` |
| DQ-03 | `transaction_status ∉ {success, failed}` | invalid | Schema |
| DQ-04 | `event_time` không phải ISO-8601 UTC | invalid | Schema |
| DQ-05 | `event_time` lớn hơn thời điểm hiện tại > 5 phút (tương lai) | invalid | Đồng hồ lỗi |
| DQ-06 | `currency` không phải mã 3 chữ hoa | invalid | Schema |
| DQ-07 | `amount` > ngưỡng cực đoan (ví dụ > p99.99 của normal) | **valid + cờ** `extreme_amount`, không loại | Amount lệch phải mạnh: giá trị lớn là tín hiệu fraud, không phải lỗi dữ liệu |
| DQ-08 | `transaction_id` đã thấy | duplicate | Idempotency |
| DQ-09 | `event_time` < watermark | late | Event-time |
| DQ-10 | Trường định danh nhạy cảm không phải hash/synthetic (ví dụ `card_id_hash` không có tiền tố `sha256:`) | invalid | Yêu cầu: không dữ liệu định danh thật |

Lưu ý: DQ-07 là ví dụ cho nguyên tắc **không loại bỏ outlier có thể là fraud** - dữ liệu giá trị lớn phải đi tiếp để chấm điểm.

## 3. Sample events (một cho mỗi loại)

Các mẫu dưới đây dùng đúng schema README; **[NTH]** thay bằng event thực từ simulator khi đã có.

**valid**
```json
{"transaction_id":"tx_20261004_000001","event_time":"2026-10-04T09:30:00Z","customer_id":"cus_00128","card_id_hash":"sha256:9f2c…","merchant_id":"merchant_0042","amount":1250000.0,"currency":"VND","channel":"web","device_id_hash":"sha256:ab31…","ip_hash":"sha256:77de…","country":"VN","transaction_status":"success"}
```

**invalid** (vi phạm DQ-02: `amount` âm; và DQ-03)
```json
{"transaction_id":"tx_20261004_000002","event_time":"2026-10-04T09:30:05Z","customer_id":"cus_00128","card_id_hash":"sha256:9f2c…","merchant_id":"merchant_0042","amount":-500.0,"currency":"VND","channel":"web","device_id_hash":"sha256:ab31…","ip_hash":"sha256:77de…","country":"VN","transaction_status":"pending"}
```

**duplicate** (cùng `transaction_id` với mẫu valid, được phát lại)
```json
{"transaction_id":"tx_20261004_000001","event_time":"2026-10-04T09:30:00Z","customer_id":"cus_00128","card_id_hash":"sha256:9f2c…","merchant_id":"merchant_0042","amount":1250000.0,"currency":"VND","channel":"web","device_id_hash":"sha256:ab31…","ip_hash":"sha256:77de…","country":"VN","transaction_status":"success"}
```

**late** (hợp lệ, nhưng `event_time` trễ 2 giờ so với dòng chảy; watermark mặc định 10 phút)
```json
{"transaction_id":"tx_20261004_000003","event_time":"2026-10-04T07:31:00Z","customer_id":"cus_00311","card_id_hash":"sha256:c801…","merchant_id":"merchant_0007","amount":320000.0,"currency":"VND","channel":"mobile","device_id_hash":"sha256:19ab…","ip_hash":"sha256:5e0f…","country":"VN","transaction_status":"success"}
```

> Ví dụ dùng `…` để rút gọn hash; khi đưa vào test thật phải dùng chuỗi hash đầy đủ.

## 4. Review chéo **[PTH]**: tính hợp lý dữ liệu của simulator

Phạm Trung Hiếu review simulator theo checklist; kết quả ghi vào cột "Kết quả" khi chạy thật.

### 4.1 Cách chạy

```bash
# Sinh file bằng simulator của NTH. Cùng seed/tham số sẽ tạo đúng cùng output.
python data-generator/producer.py --scenario normal --seed 42 --rate 20 --duration 60 --output out/normal.jsonl
python data-generator/producer.py --scenario card_testing --seed 42 --rate 20 --duration 60 --output out/card_testing.jsonl
python data-generator/producer.py --scenario velocity_fraud --seed 42 --rate 20 --duration 60 --output out/velocity_fraud.jsonl
python data-generator/producer.py --scenario device_sharing --seed 42 --rate 20 --duration 60 --output out/device_sharing.jsonl
python data-generator/producer.py --scenario high_value_anomaly --seed 42 --rate 20 --duration 60 --output out/high_value_anomaly.jsonl

python scripts/check_simulator_plausibility.py --file out/normal.jsonl --expect normal
python scripts/check_simulator_plausibility.py --file out/card_testing.jsonl --expect card_testing
python scripts/check_simulator_plausibility.py --file out/velocity_fraud.jsonl --expect velocity_fraud
python scripts/check_simulator_plausibility.py --file out/device_sharing.jsonl --expect device_sharing
python scripts/check_simulator_plausibility.py --file out/high_value_anomaly.jsonl --expect high_value_anomaly
```

Checker thoát mã 0 nếu file đúng kỳ vọng, 1 nếu không. Có thể gửi cùng event stream vào Kafka bằng cách thêm `--bootstrap-server localhost:9092 --topic transactions_raw`.

### 4.2 Checklist

| # | Hạng mục review | Tiêu chí | Kết quả |
|---|---|---|---|
| R-01 | ≥ 100 event normal liên tiếp đúng schema | `n_valid ≥ 100`, `n_invalid = 0` | ✅ 1.200 valid events |
| R-02 | Normal không chứa tín hiệu fraud | ≤ 1% thẻ/thiết bị vượt ngưỡng ở cả 4 tín hiệu | ✅ checker PASS |
| R-03 | Card testing tạo ≥ 3 giao dịch failed/thẻ trong 10 phút | `failed_10m` max ≥ 3 | ✅ checker PASS |
| R-04 | Velocity fraud tạo ≥ 5 giao dịch/thẻ trong 5 phút | `count_5m` max ≥ 5 | ✅ checker PASS |
| R-05 | Device sharing tạo ≥ 3 thẻ/thiết bị trong 1 giờ | `cards_per_device_1h` max ≥ 3 | ✅ checker PASS |
| R-06 | High-value anomaly vượt ≥ 5× median của chính thẻ | `amount_ratio` max ≥ 5 | ✅ checker PASS |
| R-07 | Phân bố `amount` normal hợp lý | Lệch phải (mean > median), p99 không phi thực tế so với mệnh giá VND, không có `amount ≤ 0` | ✅ Log-normal, positive amounts |
| R-08 | Tỷ lệ `failed` trong normal thấp và có thực tế | Tỷ lệ failed normal nhỏ (đề xuất < 5%) | ✅ 20/1.200 = 1,67% |
| R-09 | Phân bố `channel`, `country`, `currency` hợp lý | Không một giá trị chiếm 100% trừ khi có chủ đích (ví dụ VN/VND) | ✅ web/mobile/api; VN/SG; VND cố ý |
| R-10 | Fraud chiếm tỷ lệ phù hợp trong luồng hỗn hợp | Fraud là thiểu số rõ rệt (cùng bậc với ULB: dưới ~1–5% cho demo) | N/A - mỗi file kiểm thử một pattern, mixed stream thuộc giai đoạn integration |
| R-11 | Tái lập theo seed | Cùng seed + tham số ⇒ file giống hệt (so sánh checksum) | ✅ unit test |
| R-12 | Không dữ liệu định danh thật | Mọi ID nhạy cảm là hash/synthetic | ✅ schema unit test |
| R-13 | Mỗi fraud pattern có thể liên hệ feature v1 | Bảng 5 | ✅ checker tests và bảng 5 |

Ghi chú R-07/R-10: đây là kiểm tra **bằng mắt kèm số liệu từ checker**, vì mô phỏng không có "đáp án đúng" tuyệt đối; tiêu chí
ở đây là *đủ hợp lý để demo và kiểm thử feature*, không phải khớp phân bố ngân hàng thật.

## 5. Liên hệ scenario → tín hiệu → feature v1 (phục vụ demo)

| Scenario | Tín hiệu thiết kế | Feature v1 thể hiện | Cách chứng minh khi demo |
|---|---|---|---|
| `normal` | Không tín hiệu | Tất cả feature ở mức thấp | Checker `--expect normal` PASS |
| `card_testing` | Nhiều giao dịch nhỏ, thất bại liên tiếp | `failed_tx_count_10m` ↑, `tx_count_5m` ↑ | Event thứ 3 failed trong 10 phút có `failed_tx_count_10m ≥ 3` |
| `velocity_fraud` | Một thẻ giao dịch dồn dập | `tx_count_5m` ↑, `amount_sum_1h` ↑ | Event thứ 5 trong 5 phút có `tx_count_5m ≥ 5` |
| `device_sharing` | Một thiết bị dùng nhiều thẻ | `unique_cards_per_device_1h` ↑ | Event thẻ thứ 3 trên thiết bị có giá trị ≥ 3 |
| `high_value_anomaly` | Số tiền cao bất thường so với lịch sử thẻ | `amount_vs_avg_7d` ↑ | Event bất thường có tỷ lệ ≥ 5 |

## 6. Test tự động

| Tệp | Kiểm tra gì |
|---|---|
| `tests/test_simulator_plausibility.py` | Checker phân biệt normal / 4 fraud patterns; phân loại valid / invalid / duplicate / out-of-order; file thiếu → exit 2 |
| `tests/test_eda_pipeline.py` | Pipeline EDA: số liệu nhất quán, chia theo thời gian không chồng lấn, báo cáo đầy đủ, tái lập |

```bash
pytest tests/ -q
```

Test schema và simulator nằm trong `tests/test_schema.py` và `tests/test_scenarios.py`.

## 7. Hướng dẫn tái lập **[NTH]**

```bash
python -m pip install -r requirements.txt
python -m pytest tests -q

# JSONL local, dùng cho review/checker.
python data-generator/producer.py --scenario normal --seed 42 --rate 20 --duration 60 \
  --output out/normal.jsonl --quality-output out/quality_cases.jsonl

# Gửi cùng format vào Kafka khi Kafka đã sẵn sàng.
python data-generator/producer.py --scenario velocity_fraud --seed 42 --rate 20 --duration 60 \
  --bootstrap-server localhost:9092 --topic transactions_raw
```

Seed demo chuẩn là `42`; output local nằm trong `out/` (đã gitignore). `--rate` là số normal events/giây, `--duration` là số giây normal traffic. Fraud events được thêm sau baseline để bảo đảm feature history rõ ràng.

## 8. Trạng thái nghiệm thu

- [x] Rule data-quality đề xuất từ EDA (mục 2) và sample cho 4 loại event (mục 3) - **PTH**
- [x] Checker tính hợp lý và test tự động (mục 4, 6) - **PTH**
- [x] Chạy checklist R-01…R-13 trên output thật của simulator - 5 scenario PASS checker
- [x] Hướng dẫn tái lập (mục 7) và test schema/validator - **NTH**
- [x] Hai bên review chéo và đồng ý rule DQ-02: `amount <= 0` là invalid
