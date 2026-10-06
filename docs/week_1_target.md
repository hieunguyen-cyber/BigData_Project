# Mục tiêu Tuần 1: Phân tích, thiết kế và chuẩn bị dữ liệu cho FraudStream

> Phạm vi của Tuần 1 bao gồm toàn bộ khối lượng khởi động: phân tích bài toán, thiết kế hệ thống, khám phá dữ liệu, xây dựng fraud simulator và chuẩn bị data-quality rules.

## 1. Mục tiêu hoàn thành

Kết thúc tuần, dự án có một thiết kế thống nhất và một dòng giao dịch mẫu có thể tái lập. Hai thành viên phụ trách phải cung cấp đủ đầu vào kỹ thuật để các giai đoạn hạ tầng, batch processing, ML training và streaming processing được triển khai ở các tuần tiếp theo.

| Mã | Mục tiêu | Cách đánh giá | Mức chất lượng yêu cầu |
|---|---|---|---|
| W1-01 | Phân tích bài toán và kiến trúc | Review tài liệu + sơ đồ | Nêu rõ fraud scenarios, luồng Lambda và vai trò từng thành phần |
| W1-02 | Chuẩn hóa data contract | Review schema + data dictionary | Có kiểu dữ liệu, tính bắt buộc, ví dụ và validation rules cho từng trường |
| W1-03 | Khám phá dữ liệu | Review EDA report/notebook | Có tỷ lệ fraud, missing/duplicate analysis và tối thiểu 4 biểu đồ có nhận xét |
| W1-04 | Fraud simulator | Chạy producer với sample events | Có normal traffic và 4 fraud patterns; event tuân thủ schema |
| W1-05 | Data-quality rules | Chạy test dữ liệu đầu vào | Phân biệt valid, invalid, duplicate và late event rõ ràng |
| W1-06 | Kế hoạch công việc | Review task checklist | Mỗi deliverable có owner, tiêu chí hoàn thành và bằng chứng review |

## 2. Deliverables cuối tuần

| Deliverable | Nội dung tối thiểu | Owner |
|---|---|---|
| `docs/problem_and_architecture.md` | Bài toán, scope, fraud scenarios, sơ đồ Lambda | Nguyễn Trung Hiếu |
| `docs/data_dictionary.md` | JSON schema, field definitions, constraints, validation rules | Nguyễn Trung Hiếu |
| `docs/eda_report.md` hoặc notebook | EDA ULB/IEEE-CIS, biểu đồ, nhận xét và feature candidates | Phạm Trung Hiếu |
| `docs/feature_v1.md` | Danh sách feature, ý nghĩa và cách tránh data leakage | Phạm Trung Hiếu |
| `data-generator/` | Adapter dữ liệu, producer và fraud scenarios | Nguyễn Trung Hiếu |
| `tests/` hoặc `docs/simulator_test.md` | Test schema, invalid/duplicate/late events và hướng dẫn tái lập | Cả hai |

## 3. Phân công công việc

### Phạm Trung Hiếu - Data analysis và ML foundation

#### Nhiệm vụ

1. Khảo sát hai dataset: ULB Credit Card Fraud Detection và IEEE-CIS Fraud Detection.
2. Thực hiện EDA trên dataset ULB và mô tả mapping các cột phù hợp của IEEE-CIS.
3. Phân tích số lượng giao dịch, tỷ lệ fraud, missing values, duplicate rows và phân bố `amount`.
4. Tạo tối thiểu bốn biểu đồ:
   - Phân bố fraud/non-fraud.
   - Phân bố giá trị giao dịch.
   - Fraud rate theo nhóm giá trị giao dịch.
   - Fraud rate theo thời gian hoặc một feature phù hợp.
5. Đề xuất feature set v1 và metrics: Precision, Recall, F1-score, PR-AUC, False Positive Rate.
6. Xác định các nguy cơ data leakage và nguyên tắc chia train/validation/test theo thời gian.

#### Tiêu chí đánh giá độ hoàn thành

- EDA có thể chạy lại từ dữ liệu đầu vào và cho ra cùng kết luận chính.
- Mỗi biểu đồ có tiêu đề, nhãn trục, chú thích và nhận xét nghiệp vụ.
- Báo cáo nêu được fraud class imbalance và lý do không dùng accuracy làm metric duy nhất.
- Feature v1 có mô tả nguồn dữ liệu, thời điểm tính và lý do nghiệp vụ.
- Không có feature nào dùng fraud label hoặc dữ liệu tương lai để dự báo giao dịch hiện tại.

#### Tiêu chuẩn chất lượng

- Kết quả được diễn giải bằng số liệu, không chỉ đưa biểu đồ.
- Các tỷ lệ và số liệu thống kê tái kiểm tra được từ notebook/code.
- Feature list có thể chuyển trực tiếp thành Spark batch/streaming transformations ở giai đoạn sau.

#### Gợi ý phương án thực hiện

- Dùng `pandas`, `numpy`, `matplotlib` hoặc `seaborn` cho EDA ban đầu.
- Bắt đầu với ULB do kích thước nhỏ; dùng IEEE-CIS để phân tích mapping và mở rộng thiết kế.
- Tách rõ feature chỉ dùng lịch sử như `amount_vs_avg_7d`, `tx_count_5m` với thông tin có sau giao dịch.
- Lưu notebook và báo cáo ngắn trong repository để review qua pull request.

### Nguyễn Trung Hiếu - System design, data contract và fraud simulator

#### Nhiệm vụ

1. Viết tài liệu problem definition, fraud scenarios và sơ đồ Lambda Architecture.
2. Thiết kế transaction schema chuẩn với các trường: `transaction_id`, `event_time`, `customer_id`, `card_id_hash`, `merchant_id`, `amount`, `currency`, `channel`, `device_id_hash`, `ip_hash`, `country`, `transaction_status`.
3. Viết data dictionary: kiểu dữ liệu, ví dụ, tính bắt buộc, constraint và cách xử lý khi dữ liệu lỗi.
4. Xây fraud simulator có seed tái lập, tốc độ phát và thời lượng cấu hình được.
5. Tạo năm scenario: `normal`, `card_testing`, `velocity_fraud`, `device_sharing`, `high_value_anomaly`.
6. Viết rules để nhận diện record invalid, duplicate và late event; tạo sample event cho mỗi loại.

#### Tiêu chí đánh giá độ hoàn thành

- Sơ đồ có đủ ingestion, speed layer, batch layer, storage, serving và visualization.
- Schema không có dữ liệu định danh hoặc thông tin thẻ thật; các ID nhạy cảm là synthetic/hash.
- Producer tạo được tối thiểu 100 event `normal` liên tiếp đúng schema.
- Bốn fraud scenarios tạo được tín hiệu có thể đo: failure count, transaction velocity, device-card cardinality hoặc amount anomaly.
- Test phân loại đúng tối thiểu một valid event, một invalid event, một duplicate event và một late event.

#### Tiêu chuẩn chất lượng

- Event schema nhất quán giữa toàn bộ scenarios và adapter dữ liệu.
- Mỗi scenario có seed và tham số rõ ràng để tái lập demo.
- Rules và sample events đủ rõ để giai đoạn Spark Streaming hiện thực watermark, deduplication và state management.

#### Gợi ý phương án thực hiện

- Dùng `dataclass` hoặc Pydantic/JSON Schema để kiểm tra event trước khi phát.
- Dùng `transaction_id` duy nhất; giữ `card_id_hash` làm key dự kiến cho event stream.
- Tách logic sinh dữ liệu theo từng scenario thay vì viết một producer lớn.
- Dùng JSON Lines làm output local trước; cấu trúc producer sẵn sàng nhận cấu hình Kafka ở bước tiếp theo.

## 4. Cách phối hợp giữa hai thành viên

| Điểm giao | Phạm Trung Hiếu | Nguyễn Trung Hiếu | Kết quả chung |
|---|---|---|---|
| Transaction schema | Xác định trường cần cho EDA/feature | Định nghĩa schema và validation | Data dictionary thống nhất |
| Fraud patterns | Chỉ ra dấu hiệu cần phát hiện | Sinh event thể hiện đúng dấu hiệu | Scenario có thể dùng để test feature |
| Feature v1 | Mô tả công thức và tránh leakage | Bảo đảm simulator có dữ liệu đầu vào cần thiết | Feature có thể triển khai ở Spark |
| Data quality | Đề xuất kiểm tra từ EDA | Hiện thực sample/test cases | Rule set kiểm tra đầu vào |

Hai người review chéo deliverable của nhau trước khi kết thúc tuần: Phạm Trung Hiếu review tính hợp lý dữ liệu của simulator; Nguyễn Trung Hiếu review tính khả thi của feature set với streaming event schema.

## 5. Kế hoạch thực hiện theo ngày

| Ngày | Phạm Trung Hiếu | Nguyễn Trung Hiếu | Mốc kiểm tra |
|---|---|---|---|
| 1 | Tải/khảo sát dataset, lập EDA questions | Viết problem definition và architecture draft | Thống nhất fraud scenarios |
| 2 | Thống kê schema, missing/duplicates | Viết transaction schema và data dictionary draft | Review schema chung |
| 3 | Tạo 2 biểu đồ đầu, phân tích imbalance | Xây normal generator và validator | Event normal hợp lệ |
| 4 | Hoàn thiện 4 biểu đồ, mapping IEEE-CIS | Xây 4 fraud scenarios | Scenario tạo đúng pattern |
| 5 | Viết feature candidates và metrics | Thêm invalid/duplicate/late rules | Test cases có sample data |
| 6 | Kiểm tra leakage, hoàn thiện EDA report | Hoàn thiện simulator guide và tests | Review chéo |
| 7 | Sửa theo review, chốt feature v1 | Sửa theo review, chốt data contract | Demo + checklist cuối tuần |

## 6. Checklist nghiệm thu

- [ ] Tài liệu kiến trúc mô tả rõ bài toán và năm fraud scenarios.
- [ ] Data dictionary có schema, constraint và ví dụ JSON.
- [ ] EDA có tối thiểu bốn biểu đồ cùng nhận xét.
- [ ] Báo cáo EDA nêu class imbalance, missing values, duplicate records và giới hạn dữ liệu.
- [ ] Feature v1 có công thức/mô tả và kiểm tra data leakage.
- [ ] Simulator phát được normal traffic và bốn fraud patterns.
- [ ] Simulator có seed, rate và duration cấu hình được.
- [ ] Có sample/test cho valid, invalid, duplicate và late event.
- [ ] Hai deliverable được review chéo trước khi merge.

## 7. Definition of Done

Tuần 1 hoàn thành khi toàn bộ checklist được đánh dấu, mọi deliverable có trong repository, và nhóm có thể trình diễn: chọn một scenario, tạo event JSON hợp schema, chỉ ra tín hiệu fraud mà event đó được thiết kế để biểu hiện, đồng thời liên hệ tín hiệu này với feature v1 tương ứng.
