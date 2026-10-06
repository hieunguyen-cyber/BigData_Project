# Mục tiêu gộp Tuần 1-2: Khởi động, hiểu dữ liệu và tạo dòng giao dịch đầu tiên

> Dự án: **FraudStream - Real-Time Online Payment Fraud Detection**  
> Thời lượng: 02 tuần đầu của dự án, được quản lý như một sprint chung.  
> Mục đích: tạo nền tảng thống nhất để từ tuần sau nhóm có thể bắt đầu triển khai hạ tầng và pipeline Spark.

## 1. Mục tiêu hoàn thành của sprint

Kết thúc sprint, nhóm cần có một phạm vi bài toán rõ ràng, dữ liệu được khảo sát, schema giao dịch dùng chung và một luồng event JSON có thể được phát/đọc qua Kafka. Tất cả đầu ra phải có trong repository hoặc được liên kết rõ ràng từ repository.

### Tiêu chí hoàn thành chung

| Mã | Tiêu chí | Cách đánh giá | Mức chất lượng đạt yêu cầu |
|---|---|---|---|
| G1 | Tài liệu bài toán và kiến trúc | Review tài liệu và sơ đồ | Mô tả nhất quán với README, giải thích được Lambda Architecture và luồng dữ liệu |
| G2 | Schema giao dịch chuẩn | Review JSON schema + data dictionary | Có kiểu dữ liệu, tính bắt buộc, ví dụ và quy tắc validation cho từng trường |
| G3 | Hiểu dữ liệu | Review notebook/báo cáo EDA | Có số lượng bản ghi, tỷ lệ fraud, missing values, duplicates và ít nhất 4 biểu đồ có nhận xét |
| G4 | Fraud simulator | Chạy producer và quan sát message | Có ít nhất 5 scenario, event hợp schema, có thể cấu hình tốc độ và thời lượng |
| G5 | Kafka smoke test | Producer/consumer test | Consumer nhận được event JSON hợp lệ, key theo `card_id_hash`, không có lỗi parse |
| G6 | Tổ chức nhóm | Review GitHub Issues và PR | Mỗi việc có owner, tiêu chí hoàn thành; mã nguồn có review trước khi merge |

## 2. Deliverables bắt buộc

| Deliverable | Nội dung tối thiểu | Người chịu trách nhiệm chính |
|---|---|---|
| `docs/problem_and_architecture.md` | Bài toán, scope, fraud scenarios, sơ đồ Lambda | Nguyễn Minh Hoàng |
| `docs/data_dictionary.md` | Transaction schema, data types, validation rules | Nguyễn Minh Hoàng + Nguyễn Trung Hiếu |
| `docs/eda_report.md` hoặc notebook | Phân tích ULB/IEEE-CIS và biểu đồ | Phạm Trung Hiếu |
| `data-generator/` | Producer, adapters, fraud scenarios | Nguyễn Minh Hoàng + Nguyễn Trung Hiếu |
| `docs/kafka_smoke_test.md` | Topics, lệnh test, kết quả kỳ vọng | Nguyễn Trung Hiếu |
| `docs/storage_plan.md` | Raw/cleaned/curated layout, Parquet partition strategy | Lê Trọng Đạt |
| `docs/dev_environment.md` | Cách cài/chạy môi trường chung, version matrix | Lê Xuân Nhật Khôi |
| GitHub Issues/board | Backlog 8 tuần, owner và trạng thái | Lê Xuân Nhật Khôi |

## 3. Phân công chi tiết

### Nguyễn Minh Hoàng - Tech lead và data contract

**Nhiệm vụ**

1. Soạn tài liệu bài toán, phạm vi và sơ đồ Lambda Architecture.
2. Chuẩn hóa transaction schema và đưa ra nguyên tắc đặt tên fields.
3. Tạo khung `data-generator/`, adapter dữ liệu và scenario `normal`.
4. Review các deliverables để đảm bảo dùng chung một schema.

**Cách đánh giá**

- Sơ đồ có đủ Kafka, Spark speed/batch layer, HDFS, Cassandra và dashboard.
- Schema không chứa PII hoặc số thẻ thật; các ID nhạy cảm được hash.
- Producer tạo được 100 event `normal` liên tiếp đúng schema.

**Gợi ý thực hiện**

- Dùng JSON Schema hoặc Pydantic/dataclass để validate event ở producer.
- Dùng seed cố định để mỗi demo có thể tái lập.
- Dùng `transaction_id` duy nhất và Kafka key là `card_id_hash`.

### Lê Trọng Đạt - Data storage và batch-data preparation

**Nhiệm vụ**

1. Khảo sát format của ULB và IEEE-CIS, chỉ ra trường nào dùng được cho raw/curated layer.
2. Soạn kế hoạch data lake gồm raw, cleaned, curated, late-events và model artifacts.
3. Thiết kế naming convention, partition theo `event_date/event_hour` và format Parquet + Snappy.
4. Tạo sample data chuẩn hóa nhỏ để team dùng trong unit test.

**Cách đánh giá**

- Tài liệu storage plan có đường dẫn, partition key, retention và mục đích từng layer.
- Sample data có cả normal, invalid, duplicate và late event.
- Lý do chọn Parquet/partition rõ ràng, gắn với Spark query performance.

**Gợi ý thực hiện**

- Bắt đầu từ một table transaction chung; không đưa các cột ít ý nghĩa vào curated layer.
- Đặt partition theo event time, không theo processing time.
- Chuẩn bị ví dụ query theo ngày/giờ để chứng minh partition pruning ở sprint sau.

### Phạm Trung Hiếu - EDA và machine-learning foundation

**Nhiệm vụ**

1. Khảo sát ULB Credit Card Fraud Detection trước, sau đó lập bản đồ cột của IEEE-CIS.
2. Tính số lượng giao dịch, tỷ lệ fraud, missing values, duplicate rows và thống kê amount.
3. Tạo tối thiểu bốn biểu đồ: class distribution, amount distribution, fraud rate theo amount bin và một biểu đồ thời gian/feature phù hợp.
4. Đề xuất feature list v1 và baseline evaluation plan: Precision, Recall, F1, PR-AUC.

**Cách đánh giá**

- Mỗi biểu đồ có tiêu đề, trục, chú thích và nhận xét nghiệp vụ ngắn.
- Báo cáo nêu rõ vì sao accuracy không đủ cho fraud detection.
- Feature v1 không dùng fraud label hoặc dữ liệu tương lai để dự đoán giao dịch hiện tại.

**Gợi ý thực hiện**

- Chia dữ liệu theo thời gian nếu có timestamp; nếu ULB không có timestamp tuyệt đối, ghi rõ hạn chế đó.
- Dùng pandas cho EDA nhỏ; đề xuất Spark DataFrame cho bước batch scale-up.
- Lưu notebook và file export biểu đồ trong thư mục `docs/` hoặc `notebooks/`.

### Nguyễn Trung Hiếu - Kafka và streaming-data contract

**Nhiệm vụ**

1. Thiết kế bốn Kafka topics: `transactions_raw`, `transactions_invalid`, `fraud_alerts`, `fraud_feedback`.
2. Viết consumer smoke test kiểm tra parse JSON, schema validation và Kafka key.
3. Phát triển bốn scenario fraud: `card_testing`, `velocity_fraud`, `device_sharing`, `high_value_anomaly`.
4. Phối hợp với Hoàng để tích hợp tất cả scenario vào simulator và tài liệu data dictionary.

**Cách đánh giá**

- Mỗi topic có mục đích, producer, consumer, key và retention policy đề xuất.
- Consumer nhận đúng tối thiểu 100 messages; lỗi schema được thống kê tách biệt.
- Mỗi scenario tạo được tín hiệu feature có thể quan sát: velocity, failed count, device-card cardinality hoặc amount anomaly.

**Gợi ý thực hiện**

- Kafka key luôn là `card_id_hash` cho raw transaction.
- Các scenario chỉ thay đổi dữ liệu cần thiết; giữ schema đồng nhất.
- Viết smoke test ở chế độ local trước, sau đó đưa thành tài liệu lệnh chạy.

### Lê Xuân Nhật Khôi - môi trường, DevOps và quản trị dự án

**Nhiệm vụ**

1. Lập version matrix cho Python, Java, Docker và Python dependencies theo môi trường chuẩn đã ghi trong README.
2. Viết `docs/dev_environment.md`: cách tạo virtual environment, cài `requirements.txt`, kiểm tra versions và quy ước `.env`.
3. Tạo GitHub Issues/board cho roadmap 8 tuần; mỗi issue có owner, deliverable và acceptance criteria.
4. Soạn hướng dẫn chất lượng mã: branch naming, pull request template, lint và test command.

**Cách đánh giá**

- Một thành viên khác có thể làm theo tài liệu để tạo Python environment thành công.
- `requirements.txt` và version matrix khớp nhau.
- Board thể hiện toàn bộ việc tuần 1-2 và ít nhất các milestone từ tuần 3-8.

**Gợi ý thực hiện**

- Dùng `python3 --version`, `java -version`, `docker --version` để kiểm tra trước khi ghi tài liệu.
- Mọi dependency mới phải có lý do, version pin và được cập nhật đồng thời trong README/requirements.
- Dùng GitHub Projects hoặc labels `week-1-2`, `data`, `streaming`, `ml`, `infra`, `docs`.

## 4. Cân bằng nhiệm vụ cho hai Hiếu

| Tiêu chí | Phạm Trung Hiếu | Nguyễn Trung Hiếu |
|---|---|---|
| Trọng tâm | Khám phá dữ liệu, nhãn, feature và ML evaluation | Data contract streaming, Kafka và fraud event generation |
| Đầu ra chính | EDA report + feature v1 + metrics plan | Kafka design + smoke test + 4 fraud scenarios |
| Độ phức tạp | Phân tích, diễn giải dữ liệu, tránh leakage | Thiết kế event stream, producer/consumer, validation |
| Phụ thuộc | Dataset và schema chung | Schema chung, Kafka local/container |
| Cách phối hợp | Định nghĩa feature cần quan sát | Sinh scenario biểu hiện đúng các feature đó |

Hai phần việc có độ lớn tương đương và gặp nhau ở feature v1: Phạm Trung Hiếu xác định tín hiệu cần phát hiện; Nguyễn Trung Hiếu bảo đảm simulator tạo ra được các tín hiệu đó trong stream.

## 5. Kế hoạch theo ngày làm việc

| Mốc | Hoạt động | Kết quả kiểm tra |
|---|---|---|
| Ngày 1 | Kickoff, chốt scope, tạo Issues và phân công | Board có owner và hạn hoàn thành |
| Ngày 2-3 | Schema, data dictionary, EDA sơ bộ | Một sample event validate được; có thống kê dataset |
| Ngày 4-5 | Simulator normal + storage plan + dev guide | Producer phát event; tài liệu môi trường hoàn chỉnh |
| Ngày 6-7 | Fraud scenarios và Kafka smoke test | Consumer nhận và phân loại event đúng schema |
| Ngày 8-9 | Hoàn thiện EDA, feature v1, review chéo | 4 biểu đồ, feature list, PR reviews |
| Ngày 10 | Sprint review và demo | Demo Kafka + simulator; checklist đạt G1-G6 |

## 6. Checklist review cuối sprint

- [ ] README và `requirements.txt` khớp version baseline.
- [ ] Mọi thành viên đã đọc schema/data dictionary.
- [ ] Dataset và license/link được ghi rõ; dataset không bị commit vào Git.
- [ ] EDA có kết luận về class imbalance, missing data và giới hạn dataset.
- [ ] Có ít nhất năm scenario gồm normal traffic và bốn fraud patterns.
- [ ] Producer/consumer smoke test thành công với JSON hợp lệ.
- [ ] Có test event invalid, duplicate và late event.
- [ ] Kafka topics và key strategy được tài liệu hóa.
- [ ] Tất cả deliverables có link từ GitHub Issue hoặc README.
- [ ] Các PR được review và không có lỗi lint/test cơ bản.

## 7. Tiêu chuẩn chất lượng

Một deliverable chỉ được xem là hoàn thành khi:

1. Có nội dung/implementation nằm trong repository, không chỉ mô tả miệng.
2. Có hướng dẫn chạy hoặc tái tạo kết quả.
3. Có người khác review.
4. Dùng đúng transaction schema chung.
5. Không chứa dữ liệu nhạy cảm, secrets hoặc dataset gốc.
6. Có bằng chứng chạy: log, screenshot dashboard/consumer, bảng kết quả hoặc notebook output phù hợp.

## 8. Cách cập nhật tài liệu và dependencies

- Khi thêm package Python: cập nhật `requirements.txt`, README và `docs/dev_environment.md` trong cùng pull request.
- Khi đổi schema/topic/feature: cập nhật data dictionary, simulator và test liên quan trong cùng pull request.
- Khi đổi môi trường chuẩn: ghi version mới, lý do thay đổi và kiểm tra tương thích với toàn bộ nhóm.
- Mọi thay đổi quan trọng cần được phản ánh vào GitHub Issue tương ứng trước khi merge.
