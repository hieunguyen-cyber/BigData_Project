# Code walkthrough — FraudStream

Tài liệu này dùng khi **show code và vấn đáp**. Luồng nên trình bày là:

```text
data contract -> simulator -> Kafka -> HDFS/Cassandra -> test -> EDA
```

> Câu mở đầu: “FraudStream tách hợp đồng dữ liệu, logic sinh dữ liệu và hạ
> tầng. Vì thế một transaction được kiểm tra nhất quán ở simulator, batch
> fixture và luồng tích hợp.”

## 1. Bản đồ phần code quan trọng

| Phần | File | Vai trò |
|---|---|---|
| Data contract | `data-generator/schema.py` | Schema, validation, deduplicate, late-event classification. |
| Simulator | `data-generator/scenarios.py`, `producer.py` | Sinh transaction xác định được từ seed, ghi JSONL hoặc gửi Kafka. |
| Scenario checker | `scripts/check_simulator_plausibility.py` | Chứng minh scenario thực sự mang tín hiệu fraud. |
| EDA | `scripts/eda_ulb.py` | Tái lập thống kê, biểu đồ và báo cáo ULB. |
| Batch contract | `tests/fixtures/batch/v1/`, `test_batch_fixture.py` | Đóng băng input/output nhỏ để test quy tắc batch. |
| Runtime | `Makefile`, `infra/` | Deploy, kiểm tra persistence và integration. |

## 2. Data contract — `data-generator/schema.py`

Đây là file nên mở đầu tiên. Nó là **nguồn chân lý** cho transaction event.

### Hằng số

- `REQUIRED_FIELDS`: 12 trường bắt buộc: định danh giao dịch, event time,
  customer/card/merchant, amount, currency, channel, device/IP, country,
  trạng thái.
- `VALID_STATUSES = {"success", "failed"}` và `VALID_CHANNELS` đóng miền giá
  trị, tránh record mơ hồ.
- `HASH_RE`, `COUNTRY_RE`, `CURRENCY_RE` xác nhận hash SHA-256, mã quốc gia
  ISO-3166-1 alpha-2 và tiền tệ ISO-4217.

Không có số thẻ, IP hay device ID thô trong event. Chỉ có hash dạng
`sha256:<64 ký tự hex>`, giúp nhóm hành vi mà không đưa PII vào repository.

### `parse_event_time(value)`

```python
parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
if parsed.tzinfo is None:
    raise ValueError("event_time phải có timezone UTC")
return parsed.astimezone(timezone.utc)
```

`Z` được đổi thành `+00:00` để Python parse được. Timestamp không có timezone
bị từ chối, nên streaming và batch không diễn giải khác nhau theo máy chạy.

### `synthetic_hash(value)`

Tạo SHA-256 xác định: cùng input luôn ra cùng output. Nhờ vậy transaction của
cùng card/device vẫn nhóm được, nhưng không lưu dữ liệu nhạy cảm.

### `validate_event(event, now=None)`

Hàm **trả danh sách lỗi** thay vì ném lỗi ngay: pipeline có thể chuyển record
lỗi sang nhánh invalid thay vì làm dừng cả job.

| Rule | Ý nghĩa |
|---|---|
| DQ-01 | Thiếu field hoặc channel không thuộc `web/mobile/api`. |
| DQ-02 | Amount không phải số dương; bool cũng bị loại. |
| DQ-03 | Status ngoài `success/failed`. |
| DQ-04 | Event time không phải ISO-8601 UTC. |
| DQ-05 | Event ở tương lai quá 5 phút. |
| DQ-06 | Currency/country sai định dạng chuẩn. |
| DQ-10 | Card/device/IP không phải synthetic SHA-256. |

`amount = 0` bị coi invalid trong FraudStream vì contract mô hình hóa payment
event có giá trị dương. Điều này không nói ULB sai: ULB là dữ liệu nghiên cứu
ẩn danh, còn contract là quyết định của hệ thống.

### `classify_events(events, watermark_minutes=10, now=None)`

Quy tắc phân loại có thứ tự cố định:

```text
validate schema -> duplicate transaction_id -> late event -> valid
```

- `seen_ids`: bắt duplicate, mã DQ-08.
- `max_event_time`: event time lớn nhất từng thấy.
- Event hợp lệ schema nhưng cũ hơn `max_event_time - watermark` là `late`,
  mã DQ-09; nó không phải invalid.
- Duplicate được kiểm tra trước late để lỗi định danh được ghi nhận đúng.
- `json_line()` serialize JSON ổn định với `sort_keys=True`, hỗ trợ kiểm tra
  hash và tái lập JSONL.

## 3. Fraud simulator

### `data-generator/scenarios.py`

`DEFAULT_START` và `random.Random(seed)` khiến output tái lập được: cùng
scenario, seed, rate, duration sẽ sinh cùng event. Đây là lý do test/debug
không phụ thuộc vào ngẫu nhiên.

`_event()` là factory nội bộ: nhận `card`, `device`, `amount`, `status` rồi
tạo event đầy đủ, gồm các hash và timestamp UTC. `normal_events()` tạo control
traffic, dùng card/device gần như riêng biệt để normal traffic không tình cờ
vượt ngưỡng fraud.

`generate()` luôn tạo ít nhất 100 normal events, sau đó chèn pattern:

| Scenario | Pattern chèn | Signal cần vượt |
|---|---|---|
| `normal` | Không có bất thường | Không vượt fraud threshold. |
| `card_testing` | 6 giao dịch nhỏ failed, mỗi 20 giây, cùng card | `failed_10m >= 3`. |
| `velocity_fraud` | 8 giao dịch trong 3,5 phút, cùng card | `count_5m >= 5`. |
| `device_sharing` | 5 card dùng chung device trong một giờ | `cards_per_device_1h >= 3`. |
| `high_value_anomaly` | Lịch sử 100.000, sau đó 900.000 | `amount_ratio >= 5`. |

`quality_case_events()` cố ý trả về valid, invalid, duplicate, late cho test.

### `data-generator/producer.py`

`main()` parse tham số CLI, gọi `generate()`, rồi gọi `validate_event()` cho
**tất cả event trước khi xuất**. Có event lỗi là dừng để bảo vệ data contract.

- `_write_jsonl()`: ghi từng event một dòng JSONL.
- `_send_kafka()`: tạo `KafkaProducer`, gửi key là `card_id_hash`, `flush()`
  rồi luôn `close()` trong `finally`.

Key Kafka là `card_id_hash` để event của một card thường vào cùng partition;
điều này hỗ trợ giữ thứ tự khi sau này xử lý stateful theo card.

```bash
python3 data-generator/producer.py \
  --scenario velocity_fraud --seed 42 --rate 20 --duration 5 \
  --output out/velocity.jsonl --quality-output out/quality_cases.jsonl
```

## 4. Kiểm chứng simulator — `check_simulator_plausibility.py`

File này kiểm tra độc lập rằng JSONL có chứa fraud signal đúng như scenario
tuyên bố, thay vì chỉ tin vào generator.

1. `load_events()` đọc JSONL; JSON hỏng được đếm là `parse_errors`.
2. `schema_problems()` kiểm tra schema tối thiểu.
3. `compute_signals()` nhóm event theo card/device và tính signal.
4. `analyse()` tổng hợp valid, invalid, duplicate, out-of-order.
5. `verdict()` so sánh report với `--expect`, trả exit code PASS/FAIL.

`_max_window_count(times, window)` dùng two-pointer trên timestamp đã sort.
Mỗi con trỏ chỉ đi tới một lần, nên độ phức tạp O(n), không phải O(n²).

| Signal | Cách tính | Mục đích |
|---|---|---|
| `count_5m` | Max event/card trong 5 phút | Velocity fraud. |
| `failed_10m` | Max failed event/card trong 10 phút | Card testing. |
| `cards_per_device_1h` | Card phân biệt/device/giờ | Device sharing. |
| `amount_ratio` | Amount / median lịch sử card | High-value anomaly. |

Median được dùng vì ít bị outlier làm lệch baseline hơn mean.

```bash
python3 scripts/check_simulator_plausibility.py \
  --file out/velocity.jsonl --expect velocity_fraud
```

## 5. EDA tái lập — `scripts/eda_ulb.py`

File này tạo `docs/week_1/eda/eda_report.md`, `docs/week_1/eda/eda_stats.json` và các PNG từ ULB.

| Hàm | Giải thích |
|---|---|
| `load_ulb()` | Kiểm tra file và schema `Time`, `V1..V28`, `Amount`, `Class`. |
| `sha256_file()` | Lưu hash input để chứng minh report sinh từ dữ liệu nào. |
| `compute_stats()` | Tính size, fraud rate, missing, duplicate, zero amount, amount/hour/correlation/split. |
| `profile_ieee_transaction()` | Đọc IEEE-CIS theo chunk, không nạp toàn bộ file lớn vào RAM. |
| `plot_*()` | Sinh 6 biểu đồ tách biệt. |
| `build_report()` | Tạo Markdown từ số liệu; không gõ tay số trong báo cáo. |
| `run()` | Điều phối load → stats → figure → report → metadata. |

Chống leakage:

- Sort theo `Time`, chia 60% train, 20% validation, 20% test.
- Correlation với nhãn chỉ lấy từ train (`corr_source = "train"`).
- Không shuffle theo thời gian.
- Feature lịch sử chỉ dùng dữ liệu trước event hiện tại.

Câu trả lời nên dùng: “EDA tạo giả thuyết feature; amount lớn một mình không
đủ kết luận fraud vì lớp fraud mất cân bằng và cần kết hợp tín hiệu hành vi.”

## 6. Fixture batch và test

`tests/fixtures/batch/v1/transactions.jsonl` có 4 event synthetic theo thứ tự:
valid, invalid, duplicate, late. `manifest.json` chứa watermark, processing
time chuẩn, line count và cam kết không có PII. `expected_results.json` ghi
kết quả kỳ vọng.

`tests/test_batch_fixture.py` chạy `classify_events()` rồi assert:

```text
valid -> invalid -> duplicate -> late
```

Nó còn kiểm tra tổng count, DQ codes và partition date được suy ra từ UTC.
Ý nghĩa: trước khi triển khai Spark batch, dự án đã có contract test nhỏ, ổn
định và dễ review.

| Test file | Điều được bảo vệ |
|---|---|
| `test_schema.py` | Hash ổn định, thứ tự classification, không dùng sensitive ID thô. |
| `test_scenarios.py` | 5 scenario pass checker; seed tái lập output. |
| `test_simulator_plausibility.py` | Từ chối pattern sai, JSON hỏng, file mất và normal giả fraud. |
| `test_eda_pipeline.py` | EDA schema/statistics/split/correlation/report/figures. |
| `test_batch_fixture.py` | Contract xử lý batch v1. |

```bash
python3 -m pytest tests -q
```

Kết quả đã ghi nhận gần nhất là **30 passed**; lúc demo nên đọc output thật.

## 7. Hạ tầng: manifest, Makefile và smoke test

### Manifest Kubernetes

- `infra/platform/00-namespace.yaml`: namespace `fraudstream`.
- `infra/platform/01-resource-quota.yaml`: giới hạn tài nguyên cho Kind local.
- `infra/kafka/kafka.yaml`: broker Kafka + PVC.
- `infra/hdfs/hdfs-namenode.yaml`: metadata HDFS.
- `infra/hdfs/hdfs-datanode.yaml`: block dữ liệu HDFS.
- `infra/cassandra/cassandra-statefulset.yaml`: StatefulSet/PVC Cassandra.
- `infra/cassandra/cassandra-service.yaml`: endpoint mạng Cassandra.
- `infra/cassandra/cassandra-config.yaml`: cấu hình Cassandra.
- `infra/kind/kind-cluster.yaml`: cluster Kubernetes local.

Kafka, HDFS và Cassandra đều có PVC: pod bị recreate không đồng nghĩa dữ liệu
phải mất. Smoke test kiểm tra chính điều này.

### `Makefile`

Đây là entrypoint tái lập, để cả nhóm không phải nhớ chuỗi lệnh thủ công.

```text
reproduce
  └─ bootstrap ─ deploy ─ platform ─ cluster ─ check-tools
  ├─ verify
  └─ integration
```

- `check-tools`: Docker, Kind, kubectl, Python.
- `cluster`: tạo hoặc tái dùng `fraudstream-cluster`.
- `platform`: namespace, quota và preflight PVC/DNS.
- `deploy`: apply Cassandra/Kafka/HDFS, đợi rollout.
- `bootstrap`: Kafka topic, thư mục HDFS, Cassandra smoke test.
- `verify`: full verifier Bash; `integration`: end-to-end test.
- `destroy`: bắt buộc `CONFIRM=1` để chống xóa nhầm cluster/data local.

### Smoke tests

- `infra/kafka/bootstrap_topics.sh`: tạo topic idempotent.
- `infra/kafka/smoke_test.py`: gửi/đọc Kafka; tùy chọn persistence test.
- `infra/hdfs/setup_datalake.sh`: chờ HDFS ready và tạo raw/cleaned/curated/
  late_events/checkpoints/models.
- `infra/hdfs/smoke_test.sh`: kiểm tra pod, PVC, live DataNode, write/read và
  SHA-256; có mode persistence.
- `infra/cassandra/smoke_test.sh`: kiểm tra Cassandra `UN`, chạy CQL insert/
  select và có mode persistence.
- `infra/scripts/check_preflight.sh`: chặn lỗi môi trường trước deploy.
- `infra/scripts/verify_infra.sh`: full verifier cho platform, 3 dịch vụ và
  HDFS write/read.

## 8. End-to-end — `infra/scripts/integration_smoke.py`

Đây là file show khi được hỏi “hệ thống có chạy thật không?”. Nó:

1. Kiểm tra Kafka, NameNode Ready và `/fraud-data/raw` tồn tại.
2. Sinh đúng 100 normal event gần thời điểm chạy, gắn `run_id` vào transaction
   ID để không đụng lần test khác.
3. Validate 100 event bằng `validate_event()`.
4. Tạo pod Kafka client tạm trong namespace.
5. Đọc Kafka offset trước/sau produce; consume đúng số message mới.
6. So key/value Kafka với event gốc.
7. Đẩy JSONL vào `/fraud-data/raw/integration_<run_id>.jsonl` trên HDFS.
8. Đọc lại HDFS, so SHA-256, sau đó dọn pod tạm và local temp file trong
   `finally`.

Điểm cần nhấn mạnh: đây không chỉ là “pod Running”; nó kiểm tra 100 event
không mất hoặc thay đổi qua simulator → Kafka → HDFS.

## 9. Kịch bản demo 10–12 phút

1. Mở `README.md`: bài toán và Lambda Architecture.
2. Mở `schema.py`: 12 field, hash PII, DQ rules, thứ tự valid/duplicate/late.
3. Mở `scenarios.py`/`producer.py`: seed, 5 scenario, Kafka key.
4. Chạy producer + checker cho `velocity_fraud`.
5. Mở `test_batch_fixture.py`: fixture 4 event và expected result.
6. Mở `Makefile`, rồi `integration_smoke.py`: 100 events, Kafka offset, HDFS
   SHA-256.
7. Mở `docs/week_2/week_2_test_evidence.md` hoặc `docs/checkpoints/checkpoint_1.md` để show log
   runtime đã ghi nhận.

```bash
python3 -m pytest tests -q
make status
make reproduce
```

Không chạy `make destroy CONFIRM=1` trong khi demo: lệnh xóa Kind cluster và
dữ liệu local trong cluster.

## 10. Câu hỏi đáp nhanh

**Vì sao cần HDFS và Cassandra?** HDFS lưu lịch sử lớn cho data lake/batch;
Cassandra giữ score/alert đã tính để truy vấn nhanh theo key.

**Vì sao có Kafka?** Kafka tách producer-consumer, đệm burst traffic, giữ thứ
tự theo partition và cho nhiều layer đọc cùng stream.

**Late event đi đâu?** Hợp lệ schema nhưng quá watermark sẽ vào nhánh
`late_events` để đối soát, tránh làm sai state online.

**Simulator có thay dữ liệu thật không?** Không. Simulator kiểm tra logic
feature/scenario có kiểm soát; ULB dùng cho EDA/baseline trên dữ liệu thật.

**W2 chứng minh điều gì?** Storage, messaging, Kubernetes và integration đã
chạy. Spark production/ML model/dashboard là roadmap sau, không trình bày như
deliverable đã hoàn thành.
