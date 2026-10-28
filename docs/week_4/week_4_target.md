# Mục tiêu W4: Đóng W3 và hiện thực nửa đầu Batch Data Pipeline

> Phạm vi W4: hoàn tất phần còn lại của **W3 theo roadmap gốc** bằng nghiệm thu
> vận hành/handoff hạ tầng; đồng thời hiện thực **nửa đầu W4 theo roadmap gốc**:
> batch path `raw JSONL -> validated/cleaned Parquet`.
> Thành viên phụ trách: **Nguyễn Minh Hoàng** và **Lê Xuân Nhật Khôi**. Các
> thành viên khác không thuộc phạm vi kế hoạch này.

## 1. Phạm vi và mục tiêu hoàn thành

Hạ tầng nền của roadmap gốc (Kind, Kafka, HDFS, Cassandra, PVC và smoke test)
đã có runtime evidence. Phần W3 còn lại trong W4 không phải dựng lại service,
mà là chốt một điểm bàn giao có thể tái lập: owner xác nhận interface, chạy
full verification và ghi evidence trung thực để Spark batch có một runtime
đáng tin cậy.

Nửa đầu W4 tập trung vào ingestion batch và chất lượng dữ liệu: giữ raw JSONL
bất biến, parse/validate theo event contract, phân loại `invalid -> duplicate
-> late -> valid`, và ghi tập valid ra Parquet partitioned theo `event_date`.

| Mã | Mục tiêu | Cách đánh giá | Mức chất lượng yêu cầu |
|---|---|---|---|
| W4-01 | Đóng W3 — platform handoff | `make reproduce`, `make verify`, component smoke tests | Kafka/HDFS/Cassandra/PVC healthy; evidence có lệnh và output thực tế |
| W4-02 | Batch job raw -> cleaned | Spark job chạy fixture batch từ HDFS/local test source | Input JSONL được preserve; valid records thành Parquet cleaned |
| W4-03 | Quality routing | Test valid/invalid/duplicate/late và reason/metrics | Bốn loại loại trừ lẫn nhau; không double-count hay làm rơi record im lặng |
| W4-04 | Batch run manifest | JSON/Parquet manifest cho mỗi batch đã commit | Có input checksum/count, output count, schema/version, partition và status |
| W4-05 | Runtime + tài liệu tái lập | Submit/verify từ repository root | Cấu hình, lệnh chạy, expected output và known limits rõ ràng |

### Phần cố ý chưa thuộc W4 này

- Curated feature enrichment đầy đủ, feature store và model training/evaluation.
- Spark Structured Streaming, online feature windows, alert topic và online
  serving write.
- Dashboard, monitoring/benchmark chuyên sâu, HA, TLS/SASL/ACL.

`curated` không được tuyên bố là feature-bearing output trong W4 này. Nếu cần
tạo path/placeholder để kiểm tra layout, nó phải ghi rõ là chưa phải curated
dataset đã enrich.

## 2. Deliverables cuối W4

| Deliverable | Nội dung tối thiểu | Owner |
|---|---|---|
| W3 platform handoff evidence | Kết quả `reproduce/verify`, component smoke/persistence và cross-review interface | Hoàng + Khôi |
| `spark-batch/` hoặc module batch tương đương | Entry point Spark, config versioned, đọc raw JSONL, validation/classification và cleaned writer | Hoàng |
| Batch fixture runner + tests | Chạy fixture valid/invalid/duplicate/late; assert counts, paths, schema, partition | Hoàng |
| Spark runtime packaging/deployment | Image/dependency lock, K8s submit manifest hoặc lệnh submit chuẩn, HDFS endpoint/config | Khôi |
| Batch run manifest + verifier | Kiểm tra raw immutable, checksum, cleaned Parquet, `event_date`, metrics và failure semantics | Khôi |
| `docs/week_4/` | Runbook, evidence, limitations và integration contract cập nhật | Hoàng + Khôi |

## 3. Phân công chi tiết

### Nguyễn Minh Hoàng — Batch transformation và data-quality implementation

#### Nhiệm vụ

1. **Đóng W3 theo ownership Kafka/storage handoff:** chạy lại `make reproduce`,
   `make verify` và Kafka smoke test; kiểm tra topic, key `card_id_hash`, raw
   input path và các giới hạn local. Bàn giao output thật cho evidence chung.
2. Tạo entry point batch có cấu hình versioned: `batch_id`, raw input path,
   output root, processing-time reference và schema version. Không hard-code
   host endpoint hoặc ngày chạy vào mã nguồn.
3. Hiện thực các stage theo contract hiện có: preserve raw manifest, parse
   JSONL, validate 12 fields, chuẩn hoá `event_time` UTC, rồi phân loại đúng
   thứ tự `invalid -> duplicate -> late -> valid`.
4. Ghi **chỉ** tập valid thành cleaned Parquet + Snappy, partition
   `event_date=YYYY-MM-DD`; đảm bảo `event_date` được suy ra từ UTC event time.
5. Viết test/fixture runner cho bốn loại record; assert metric reconciliation,
   unique `transaction_id`, schema và partition. Test phải fail nếu writer
   double-count hoặc ghi invalid/late/duplicate vào cleaned.
6. Cập nhật tài liệu implementation và bàn giao interface job/config/expected
   manifest để Khôi đóng gói và chạy trên Kubernetes.

#### Tiêu chí đánh giá độ hoàn thành

- Fixture 4 record cho kết quả: `input=4`, `valid=1`, `invalid=1`,
  `duplicate=1`, `late=1`, `cleaned=1`.
- Raw source checksum và line count có mặt trong run manifest; raw input không
  bị biến đổi bởi job.
- Cleaned output đọc được bằng Spark/Parquet reader, có đủ 12 field contract
  và `event_date`, không có `transaction_id` trùng.
- Các test chạy được từ repository root và trả exit code khác 0 khi vi phạm
  contract.

#### Tiêu chuẩn chất lượng

- Không dùng max event-time của toàn batch để phân loại late event nếu fixture
  có source order; không được dùng dữ liệu tương lai.
- Deduplicate chỉ sau validation; ID trùng nhưng payload mâu thuẫn phải có
  đường lỗi/quarantine rõ ràng, không chọn ngẫu nhiên.
- Không đưa label fraud/chargeback/review outcome làm feature hay output claim.

#### Gợi ý phương án thực hiện

- Bắt đầu bằng fixture nhỏ đã versioned; sau đó chạy lại cùng một fixture qua
  HDFS raw path.
- Tái dùng schema/quality rules trong `data-generator/` thay vì tạo schema thứ
  hai không tương thích.
- Ghi output vào thư mục run-specific tạm, validate rồi mới commit/rename.

### Lê Xuân Nhật Khôi — Batch runtime, observability và nghiệm thu platform

#### Nhiệm vụ

1. **Đóng W3 theo ownership platform/Cassandra:** review lại namespace,
   resource quota, DNS, PVC, HDFS/Cassandra readiness; chạy component smoke và
   persistence test phù hợp. Đối chiếu rằng output batch không dựa vào port
   forwarding hoặc thao tác thủ công.
2. Đóng gói runtime Spark batch: dependency/version lock, image hoặc submit
   environment, K8s job/submit manifest, CPU/memory requests-limits, ConfigMap
   cho HDFS endpoint và output root. Không commit secret.
3. Viết runner/runbook để submit batch từ repository root và chờ job terminal
   state. Job failure phải trả lỗi rõ; không ghi `PASS` chỉ vì pod được tạo.
4. Hiện thực run manifest/verifier: kiểm raw checksum/count, output Parquet
   tồn tại, output schema, partition `event_date`, metric reconciliation và
   trạng thái commit. Chỉ artifact đã commit mới được báo thành công.
5. Chạy integration batch cùng Hoàng bằng fixture: HDFS raw -> Spark batch ->
   HDFS cleaned. Lưu output thực tế và checksum/metrics trong evidence doc.
6. Review chéo transformation của Hoàng, tập trung vào reproducibility,
   resources, idempotent rerun và đường đi của invalid/duplicate/late;
   cập nhật deployment/runbook/limitations.

#### Tiêu chí đánh giá độ hoàn thành

- Có một lệnh được tài liệu hoá để submit batch job, một lệnh verify output;
  cả hai chạy từ root repository.
- Spark job có trạng thái terminal thành công chỉ khi cleaned output và manifest
  hợp lệ; job lỗi phải để lại log/exit status có thể chẩn đoán.
- HDFS có raw fixture, cleaned Parquet và manifest của cùng `batch_id`; verifier
  xác nhận schema/count/checksum và `event_date` partition.
- Rerun cùng source + `batch_id` không tạo output mâu thuẫn; checksum khác với
  cùng `batch_id` phải bị chặn hoặc fail rõ ràng.

#### Tiêu chuẩn chất lượng

- Không biến Docker/Kubernetes manifest thành môi trường thứ hai với schema hay
  path khác với batch contract.
- Resource requests/limits đủ nhỏ cho Kind local nhưng được ghi rõ; không che
  OOM/Pending bằng retry vô hạn.
- Evidence có timestamp/run ID, command, exit result và limitation; không dùng
  output minh họa thay cho kết quả chạy thật.

#### Gợi ý phương án thực hiện

- Chạy local Spark trước để bóc lỗi logic, sau đó containerize/submit lên Kind
  để xác nhận HDFS integration.
- Dùng ConfigMap/arguments cho `batch_id`, input/output roots, không nhúng vào
  image.
- Tận dụng PVC/HDFS và verifier W2 đã có; không sửa manifests stateful trừ khi
  có blocker được ghi lại.

## 4. Cân bằng khối lượng và điểm giao

| Tiêu chí | Nguyễn Minh Hoàng | Lê Xuân Nhật Khôi |
|---|---|---|
| W3 closing | Kafka/raw contract, reproduce/verify evidence | Platform/Cassandra/PVC readiness, persistence evidence |
| Nửa đầu W4 | Transformation logic, data quality, Parquet writer, fixture tests | Spark packaging/submit, runtime config, manifest/verifier, HDFS integration |
| Quality ownership | Temporal integrity, dedup semantics, output correctness | Reproducibility, resource safety, commit/failure semantics |
| Review chéo | Review runtime/runbook của Khôi | Review transformation/idempotency của Hoàng |

**Điểm giao bắt buộc:** Hoàng chốt interface job/config và expected metrics;
Khôi chốt cách submit/verify trong cluster. Cả hai chạy cùng một `batch_id` và
fixture trước khi đánh dấu nghiệm thu.

## 5. Trình tự thực hiện

| Pha | Hoàng | Khôi | Mốc chung |
|---|---|---|---|
| A. Đóng W3 | Chạy reproduce/verify và Kafka handoff | Rà platform/Cassandra/HDFS readiness + persistence | Evidence platform nhất quán, blocker được ghi rõ |
| B. Logic batch | Implement parser, validation, routing, cleaned writer | Chuẩn bị Spark runtime, config và job manifest | Job interface/path/metric được thống nhất |
| C. Tích hợp | Chạy fixture, sửa data-quality mismatch | Submit trên Kind, verify HDFS/manifest | Raw -> cleaned Parquet chạy thật |
| D. Nghiệm thu | Test contract + review runtime | Verify output + review transformation | Docs/evidence cập nhật, không overclaim scope |

## 6. Checklist nghiệm thu

- [ ] `make reproduce` và `make verify` được chạy lại; Kafka/HDFS/Cassandra/PVC có evidence bàn giao.
- [ ] Có Spark batch entry point và configuration versioned.
- [ ] Raw JSONL được preserve cùng checksum/line count/`batch_id` trong manifest.
- [ ] Validation/routing bao phủ invalid, duplicate, late, valid; metric tổng khớp input.
- [ ] Chỉ valid records vào cleaned Parquet Snappy partition theo UTC `event_date`.
- [ ] Spark batch chạy qua HDFS trong môi trường đã tài liệu hóa và output được verifier kiểm tra.
- [ ] Rerun idempotent hoặc fail rõ ràng khi `batch_id` trùng với checksum mâu thuẫn.
- [ ] Hoàng và Khôi review chéo phần việc của nhau; docs ghi cả limitation còn lại.

## 7. Definition of Done

W4 này đạt khi một thành viên mới có thể, chỉ dựa vào repository:

1. Tái lập và xác minh platform stateful đã bàn giao từ W3.
2. Đặt một JSONL fixture vào raw input theo contract.
3. Submit batch job để tạo cleaned Parquet partitioned theo `event_date`.
4. Đọc run manifest để đối chiếu input, invalid, duplicate, late, valid và
   cleaned counts.
5. Chạy verifier để xác nhận output/schema/partition/checksum, đồng thời hiểu
   rõ curated feature enrichment, ML và streaming vẫn là phần tiếp theo.
