# Mục tiêu Tuần 3: Hoàn tất Hạ tầng và Chuẩn bị Batch Foundation

> Hạn chót: **thứ Sáu, 23/10/2026**. Tuần này tiếp tục xử lý phần W2 còn
> thiếu và thực hiện phần đầu của Tuần 4 roadmap: xác định đường đi
> `raw -> cleaned -> curated Parquet` cho batch pipeline.
>
> Thành viên phụ trách: **Phạm Trung Hiếu, Nguyễn Trung Hiếu và Lê Trọng Đạt**.
> Các thành viên khác không thuộc phạm vi Tuần 3.

## 1. Mục tiêu hoàn thành

Kết thúc tuần, FraudStream có HDFS chạy trong Kubernetes với persistent
storage và smoke test I/O; toàn bộ W2 có thể verify end-to-end. Nhóm cũng có
batch foundation nhất quán với data contract và checkpoint đã tổng hợp, dùng
trực tiếp để trình bày.

| Mã | Mục tiêu | Cách đánh giá | Mức chất lượng yêu cầu |
|---|---|---|---|
| W3-01 | Hoàn tất HDFS | Deploy + I/O/persistence smoke test | NameNode/DataNode, PVC, 6 paths, checksum healthy |
| W3-02 | Hoàn tất W2 integration | `verify_infra` end-to-end | Kubernetes, Kafka, Cassandra, HDFS đều PASS |
| W3-03 | Batch foundation | Review design + fixture | Input/output, quality, partitioning, metrics rõ ràng |
| W3-04 | Checkpoint 1 | Review tài liệu + rehearsal | Một owner sở hữu narrative, evidence và demo flow |

## 2. Deliverables cuối tuần

| Deliverable | Nội dung tối thiểu | Owner |
|---|---|---|
| `infra/hdfs/` | NameNode, DataNode, Service, PVC, layout bootstrap, smoke test | Lê Trọng Đạt |
| `infra/scripts/verify_infra.*` | HDFS theo manifests thật; full verification PASS | Nguyễn Trung Hiếu |
| `docs/week_3/batch_streaming_handoff.md` | Contract Kafka -> HDFS -> batch; invalid/duplicate/late routing | Nguyễn Trung Hiếu |
| `docs/week_3/batch_pipeline_v1.md` | Raw -> cleaned -> curated Parquet và acceptance metrics | Phạm Trung Hiếu |
| Fixture batch versioned | Valid, invalid, duplicate, late events | Phạm Trung Hiếu |
| `docs/checkpoints/checkpoint_1.md` | Báo cáo W1+W2, demo outline, evidence đã rà soát | Nguyễn Trung Hiếu |
| W2 deployment/evidence docs | HDFS/full verification evidence thật | Đạt + Nguyễn Trung Hiếu |

## 3. Phân công chi tiết

### Lê Trọng Đạt — HDFS và Data-lake Foundation

#### Nhiệm vụ

1. Hoàn tất toàn bộ W2-03 bị trễ: tạo `infra/hdfs/` với manifests versioned
   cho NameNode, DataNode, Services, PVC trong namespace `fraudstream`.
2. Khai báo CPU/memory requests/limits, readiness/liveness probes, DNS nội bộ;
   không dùng `emptyDir` cho metadata/data cần tồn tại qua restart.
3. Viết script idempotent tạo sáu paths: `/fraud-data/raw`, `/cleaned`,
   `/curated`, `/late_events`, `/checkpoints`, `/models`.
4. Viết smoke test: upload JSONL simulator, `ls`, `cat`, kiểm tra SHA-256 và
   persistence sau recreate client pod.
5. Cập nhật HDFS trong deployment guide/test evidence bằng output chạy thật.
6. Bàn giao labels, Service DNS, pod names và commands cho Nguyễn Trung Hiếu.

#### Tiêu chí đánh giá độ hoàn thành

- NameNode/DataNode Ready và PVC Bound.
- Có đủ sáu thư mục trên HDFS.
- File JSONL đọc lại có nội dung/checksum bằng file nguồn.
- Recreate client pod không mất file hoặc paths.
- Lệnh chạy từ root repository, lỗi rõ ràng khi test thất bại.

#### Tiêu chuẩn chất lượng

- Không có bước cấu hình thủ công không được ghi lại.
- Storage persistence không nằm hoàn toàn trên `emptyDir`.
- Script chạy lặp lại an toàn và evidence là kết quả thật, không phải log mẫu.

#### Gợi ý phương án thực hiện

- Bắt đầu một NameNode và một DataNode cho local demo, ghi rõ RF=1.
- Dùng client pod hoặc `kubectl exec` chạy `hdfs dfs -put/-ls/-cat`.
- Dùng JSONL simulator làm raw input; Parquet thuộc phần batch tuần sau.

### Nguyễn Trung Hiếu — Integration Contract và Atomic Checkpoint Task

#### Nhiệm vụ

1. Review HDFS manifests của Đạt: namespace, DNS, PVC, resources, probe,
   idempotency, security và khả năng chạy local.
2. Hoàn tất W2-05: cập nhật/kiểm thử `verify_infra.sh` và `verify_infra.ps1`
   theo labels, Service, paths HDFS thật. Full verify fail đúng khi thiếu
   dependency và PASS khi tất cả healthy.
3. Viết `docs/batch_streaming_handoff.md`: Kafka topic -> HDFS raw,
   event-time UTC, watermark 10 phút, Kafka key `card_id_hash`,
   `transaction_id` idempotency và routing invalid/duplicate/late.
4. Ghi evidence full verifier khi cluster đủ thành phần.
5. **Atomic task Checkpoint 1:** sở hữu trọn vẹn việc rà soát, tổng hợp và
   chuẩn bị thuyết trình W1+W2 trong `docs/checkpoint_1.md`: kiểm tra số liệu,
   chốt narrative, chọn code/log/biểu đồ để show, chuẩn bị demo command và
   rehearsal toàn bộ flow. Các thành viên khác chỉ bàn giao evidence kỹ thuật.

#### Tiêu chí đánh giá độ hoàn thành

- Verifier PASS platform, Kafka, Cassandra, HDFS và sáu paths.
- Handoff doc không mơ hồ về topic, key, path, event-time hoặc routing.
- Checkpoint phân biệt đúng: đã chạy, có mã nhưng chưa chạy, chưa hoàn thành.
- Có flow demo ngắn từ simulator đến Kafka/HDFS evidence chạy từ repo root.

#### Tiêu chuẩn chất lượng

- Không che giấu dependency thiếu bằng warning/PASS giả.
- Không tuyên bố Spark/model/HDFS đã hoàn thành nếu thiếu evidence.
- Checkpoint ưu tiên sơ đồ, số liệu, log PASS ngắn, code snippets nhỏ.

#### Gợi ý phương án thực hiện

- Dùng checklist W2 làm khung full verifier.
- Dùng `checkpoint_1.md` làm single source of truth.
- Rehearsal 8–10 phút: bài toán -> kiến trúc -> EDA -> simulator -> W2
  evidence -> hạn chế -> roadmap.

### Phạm Trung Hiếu — Batch Data-quality Foundation

#### Nhiệm vụ

1. Review HDFS layout theo góc nhìn batch ETL: lifecycle raw, cleaned,
   curated, late events, checkpoints, model artifacts.
2. Viết `docs/batch_pipeline_v1.md`: input JSONL raw, schema validation,
   dedup `transaction_id`, routing invalid/late, output Parquet và partition
   theo `event_date`.
3. Tạo fixture batch versioned: valid, invalid, duplicate, late event theo
   data contract; không đưa dataset thật/PII vào repository.
4. Viết acceptance metrics: input/valid/invalid/duplicate/late, output schema,
   partition layout và anti-leakage checks.
5. Review phần EDA/feature trong checkpoint do Nguyễn Trung Hiếu tổng hợp;
   bàn giao số liệu, biểu đồ, giới hạn dataset đã xác minh.

#### Tiêu chí đánh giá độ hoàn thành

- Batch design truy vết từ data contract đến HDFS paths/Parquet output.
- Fixture bao phủ bốn loại record và tái lập được.
- Mỗi metric batch có định nghĩa input/output rõ ràng.
- Partition, dedup, late-event handling không dùng dữ liệu tương lai.

#### Tiêu chuẩn chất lượng

- Không nhầm feature simulator với metric model trên dữ liệu thật.
- Không đưa label fraud vào feature tại thời điểm scoring.
- Tài liệu đủ cụ thể để hiện thực Spark tuần sau mà không phải chốt lại schema/path.

#### Gợi ý phương án thực hiện

- Tái sử dụng `data_dictionary.md`, `feature_v1.md`, quality cases hiện có.
- Dùng `event_date` từ `event_time` UTC làm partition ban đầu.
- Raw giữ JSONL nguyên bản; cleaned/curated dùng Parquet ở batch phase.

## 4. Cách phối hợp giữa ba thành viên

| Điểm giao | Đạt | Nguyễn Trung Hiếu | Phạm Trung Hiếu | Kết quả chung |
|---|---|---|---|---|
| HDFS contract | Services, paths, smoke command | Verifier/handoff | Review batch need | HDFS dùng được |
| Data quality | Raw file test | Routing handoff | Fixture/metrics | Batch input thống nhất |
| Full verification | Deploy/evidence HDFS | Chạy/ghi verifier | Review metrics | W2 PASS |
| Checkpoint 1 | Bàn giao HDFS evidence | **Sở hữu narrative, demo, rehearsal** | Bàn giao EDA/feature evidence | Báo cáo thống nhất |

Đạt và Phạm Trung Hiếu không phải chuẩn bị bài thuyết trình tổng thể; họ bàn
giao evidence đúng hạn. Nguyễn Trung Hiếu chịu trách nhiệm toàn bộ checkpoint.

## 5. Kế hoạch theo ngày

| Ngày | Lê Trọng Đạt | Nguyễn Trung Hiếu | Phạm Trung Hiếu | Mốc chung |
|---|---|---|---|---|
| Thứ Tư 21/10 | Deploy NameNode/DataNode/PVC | Review HDFS, handoff + checkpoint outline | Chốt raw/cleaned/curated contract | Pods/PVC rõ trạng thái |
| Thứ Năm 22/10 | Layout, I/O, checksum, persistence | Full verifier/evidence/checkpoint | Batch design, fixture, metrics | Docs/evidence cập nhật |
| Thứ Sáu 23/10 | Sửa blocker, chốt docs | Review chéo, handoff, rehearsal | Review batch design | Demo nội bộ |

## 6. Checklist nghiệm thu

- [x] `infra/hdfs/` có NameNode, DataNode, Service, PVC, probes, resources.
- [x] HDFS có sáu paths, I/O/checksum smoke test và persistence trên Kind cluster.
- [x] Full `verify_infra.sh`/`.ps1` kiểm tra PASS các tầng W2 trên Kind cluster.
- [x] Có `batch_streaming_handoff.md`, `batch_pipeline_v1.md` và fixture batch.
- [ ] Checkpoint do Nguyễn Trung Hiếu tổng hợp có flow demo/evidence/code/biểu đồ.
- [ ] Đạt/Phạm Trung Hiếu review đúng phạm vi và bàn giao evidence.
- [ ] Evidence docs là kết quả chạy thật, không dùng output mẫu.

## 7. Definition of Done

Tuần 3 hoàn thành khi một thành viên mới có thể, chỉ dựa vào repository:

1. Deploy HDFS và chạy I/O/persistence smoke test.
2. Chạy full verifier cho Kubernetes, Kafka, Cassandra, HDFS.
3. Hiểu raw JSONL sẽ thành cleaned/curated Parquet thế nào ở tuần batch.
4. Mở `checkpoint_1.md` và thực hiện được flow thuyết trình/demo đã chuẩn bị.
5. Phân biệt được evidence đã chạy với kế hoạch chưa triển khai.
