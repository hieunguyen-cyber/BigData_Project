# Feature v1 - FraudStream

> Owner: Phạm Trung Hiếu · Deliverable W1-03 · Reviewer: Nguyễn Trung Hiếu (tính khả thi với streaming schema)
> Trạng thái: **v1 - đã review chéo ngày 2026-10-11**. Căn cứ: README (mục 7, 10, 11), `eda_report.md`, `data_dictionary.md`.

## 1. Mục đích và phạm vi

Tài liệu đặc tả tập feature v1 dùng cho (a) rule-based scoring, (b) huấn luyện Logistic Regression/Random Forest
ở Tuần 5 và (c) tính online bằng Spark Structured Streaming ở Tuần 6. Mỗi feature được viết sao cho **chuyển trực
tiếp** được thành Spark transformation, và có cùng định nghĩa ở batch và streaming để tránh training/serving skew.

## 2. Quy ước chung (áp dụng cho mọi feature)

| Quy ước | Định nghĩa |
|---|---|
| Thời điểm tham chiếu `t` | `event_time` của giao dịch đang chấm điểm (UTC) |
| Cửa sổ `W` | Khoảng `(t − W, t]`, tức **gồm** giao dịch hiện tại và chỉ các giao dịch có `event_time ≤ t` |
| Feature "lịch sử" | Dùng khoảng `[t − H, t)` - **loại** giao dịch hiện tại (áp dụng cho nhóm baseline: `amount_vs_avg_7d`, `is_new_*`) |
| Đồng thời (cùng `event_time`) | Phá hoà bằng `transaction_id` tăng dần; giao dịch có id lớn hơn được coi là xảy ra sau |
| Chỉ dùng event-time | Không dùng processing-time để định nghĩa cửa sổ |
| Key | `card_id_hash` là key chính; `device_id_hash` cho feature theo thiết bị |
| Cold-start | Thẻ/khách chưa có lịch sử → giá trị mặc định ghi ở cột "Cold-start", đồng thời có cờ `has_history_*` |
| Giao dịch `failed` | Vẫn tính vào `tx_count_5m`; chỉ `transaction_status = failed` mới tính vào `failed_tx_count_10m` |
| Kiểu dữ liệu | Count: `int`; tổng tiền: `double`; cờ: `int` 0/1; tỷ lệ: `double` |

## 3. Đặc tả feature

### 3.1 Nhóm velocity / behaviour (cửa sổ ngắn, cần state trong streaming)

| Feature | Công thức | Key & cửa sổ | Nguồn (schema) | Fraud pattern | Cold-start | Gợi ý Spark |
|---|---|---|---|---|---|---|
| `tx_count_5m` | `COUNT(*)` các giao dịch của thẻ trong `(t−5m, t]` | `card_id_hash`, 5 phút | `card_id_hash`, `event_time` | Velocity fraud | 1 (chỉ giao dịch hiện tại) | Batch: `Window.partitionBy(card).orderBy(ts).rangeBetween(-300, 0)`; Streaming: stateful aggregation + watermark |
| `amount_sum_1h` | `SUM(amount)` của thẻ trong `(t−1h, t]` | `card_id_hash`, 1 giờ | `amount`, `card_id_hash`, `event_time` | Velocity / high-value | `amount` hiện tại | `rangeBetween(-3600, 0)` + `sum` |
| `failed_tx_count_10m` | `COUNT(*)` giao dịch `failed` của thẻ trong `(t−10m, t]` | `card_id_hash`, 10 phút | `transaction_status`, `card_id_hash`, `event_time` | Card testing | 0 hoặc 1 nếu giao dịch hiện tại failed | `count(when(status=='failed'))` trên window |
| `unique_cards_per_device_1h` | `COUNT(DISTINCT card_id_hash)` dùng cùng thiết bị trong `(t−1h, t]` | `device_id_hash`, 1 giờ | `device_id_hash`, `card_id_hash`, `event_time` | Device sharing | 1 | Batch: `size(collect_set(card))` trên window; **Streaming:** không dùng được `countDistinct` trên window kiểu trượt → dùng `approx_count_distinct` hoặc `flatMapGroupsWithState` (xem 6) |

### 3.2 Nhóm baseline hành vi (lịch sử dài, loại giao dịch hiện tại)

| Feature | Công thức | Key & cửa sổ | Nguồn | Fraud pattern | Cold-start | Gợi ý Spark |
|---|---|---|---|---|---|---|
| `amount_vs_avg_7d` | `amount / AVG(amount)` của thẻ trong `[t−7d, t)`, **không** gồm giao dịch hiện tại | `card_id_hash`, 7 ngày | `amount`, `card_id_hash`, `event_time` | High-value anomaly | `1.0` và `has_history_7d = 0` khi chưa có giao dịch nào trong 7 ngày | Window `rangeBetween(-7*86400, -1)` (loại hàng hiện tại) hoặc join với bảng profile đã tính trước (broadcast join) |
| `is_new_merchant` | `1` nếu `merchant_id` chưa từng xuất hiện với `customer_id` trong lịch sử trước `t`, ngược lại `0` | `customer_id`, toàn bộ lịch sử có sẵn | `merchant_id`, `customer_id`, `event_time` | Hành vi lạ | `1` nhưng kèm `has_history_customer = 0` (không coi là rủi ro cao nếu khách mới hoàn toàn) | Left-anti join với profile (customer, merchant) đã có trước `t`; streaming: state hoặc lookup Cassandra |
| `is_new_country` | `1` nếu `country` chưa từng xuất hiện với `customer_id` trước `t` | `customer_id` | `country`, `customer_id`, `event_time` | Hành vi lạ | Như trên | Như trên |

### 3.3 Feature bổ sung đề xuất (v1.1, chưa bắt buộc cho Tuần 5)

| Feature | Công thức | Lý do | Ưu tiên |
|---|---|---|---|
| `log_amount` | `ln(1 + amount)` | Amount lệch phải mạnh (EDA mục 6.2) | Cao - rẻ, hữu ích cho mô hình tuyến tính |
| `hour_of_day` | Giờ của `event_time` theo múi giờ chủ thẻ (hoặc UTC nếu chưa có múi giờ) | Fraud rate khác nhau theo giờ (EDA mục 6.4); cần giờ thật, không phải giờ tương đối của ULB | Trung bình |
| `time_since_last_tx_card_s` | `t − event_time` của giao dịch trước đó của thẻ (giây) | Velocity chính xác hơn đếm cửa sổ | Trung bình |
| `failed_ratio_10m` | `failed_tx_count_10m / tx_count_10m` | Chuẩn hoá card testing | Thấp |
| `unique_merchants_per_card_1h` | Số merchant khác nhau của thẻ trong 1 giờ | Quét merchant | Thấp |
| `has_history_7d`, `has_history_customer` | Cờ cold-start | Cho phép mô hình/rule phân biệt "chưa biết" với "bình thường" | Cao |

## 4. Khả năng tính theo dataset

| Feature | ULB | IEEE-CIS | Simulator | Ghi chú |
|---|---|---|---|---|
| `log_amount` | ✅ | ✅ | ✅ | |
| `tx_count_5m`, `amount_sum_1h` | ❌ không có thẻ | ⚠️ qua proxy thẻ (`card1–card6`+`addr1`), `TransactionDT` tương đối | ✅ | Proxy có nhiễu: nhiều người dùng chung một tổ hợp `card*` |
| `failed_tx_count_10m` | ❌ | ❌ không có giao dịch failed | ✅ duy nhất | Chỉ kiểm chứng được bằng simulator |
| `unique_cards_per_device_1h` | ❌ | ⚠️ proxy thiết bị, thiếu nhiều | ✅ | |
| `amount_vs_avg_7d` | ❌ (chỉ 2 ngày, không có thẻ) | ⚠️ proxy thẻ | ✅ | Cần simulator sinh lịch sử ≥ 7 ngày |
| `is_new_merchant` | ❌ | ❌ không có merchant thật | ✅ | |
| `is_new_country` | ❌ | ⚠️ `addr2` ẩn danh | ✅ | |

**Hệ quả quan trọng:** huấn luyện **mô hình supervised bằng nhãn thật** chỉ thực hiện được trên ULB (feature: `Amount`,
`V1–V28`, ẩn danh) và IEEE-CIS (feature ẩn danh/proxy). Feature v1 hành vi là feature của **rule engine và mô hình trên
dữ liệu simulator**; không được tuyên bố độ chính xác của chúng như thể đã kiểm chứng trên dữ liệu thật.

## 5. Ngưỡng rule-based khởi điểm (đề xuất, cần hiệu chỉnh ở Tuần 5–6)

Đây là điểm xuất phát để Nguyễn Trung Hiếu cấu hình simulator và checker; **không** rút từ dữ liệu thật.

| Điều kiện | Ngưỡng đề xuất | Dùng làm |
|---|---|---|
| `tx_count_5m` | ≥ 5 | Tín hiệu velocity |
| `failed_tx_count_10m` | ≥ 3 | Tín hiệu card testing |
| `unique_cards_per_device_1h` | ≥ 3 | Tín hiệu device sharing |
| `amount_vs_avg_7d` | ≥ 5 | Tín hiệu high-value |
| Ngưỡng `REVIEW` / `BLOCK` theo `risk_score` | Chọn trên **validation** theo mục 7, không chọn trên test | Quyết định `APPROVE/REVIEW/BLOCK` |

Các ngưỡng này khớp mặc định của `scripts/check_simulator_plausibility.py`.

## 6. Khả thi triển khai (để review chéo)

| Feature | Batch (Spark) | Streaming (Structured Streaming) | Rủi ro/chú ý |
|---|---|---|---|
| `tx_count_5m`, `amount_sum_1h`, `failed_tx_count_10m` | Window function theo range | Cần state theo thẻ; cửa sổ **trượt theo từng event** không có sẵn dạng `window()`; dùng `flatMapGroupsWithState`/`applyInPandasWithState` hoặc xấp xỉ bằng sliding window (`window("5 minutes","1 minute")`) | Chọn xấp xỉ phải được ghi rõ vì khác định nghĩa "tính tại từng event" ở batch |
| `unique_cards_per_device_1h` | `collect_set` + `size` | `approx_count_distinct` hoặc state set | Độ chính xác của xấp xỉ; kích thước state |
| `amount_vs_avg_7d` | Window 7 ngày | Đọc profile 7 ngày từ Cassandra/HDFS (stream-static join) rồi cập nhật | Profile phải được cập nhật **sau** khi chấm điểm giao dịch |
| `is_new_merchant/country` | Left-anti join | Stream-static join với profile nhỏ (broadcast) | Phụ thuộc độ tươi của profile |

**Cân bằng batch-streaming:** để huấn luyện (batch) và phục vụ (streaming) dùng cùng định nghĩa, nên tính feature huấn luyện
bằng đúng cách mô phỏng "tại thời điểm `t` chỉ biết dữ liệu đã đến" (xem mục 7.4).

**Dữ liệu đến muộn:** event đến muộn vượt watermark vào `late_events`; feature của event đó được tính lại ở batch layer,
có thể khác giá trị online. Ghi nhận là nguồn sai khác chấp nhận được và cần theo dõi số lượng late.

## 7. Kiểm tra data leakage

### 7.1 Nguyên tắc

1. **Không dùng nhãn:** không feature nào được dẫn xuất từ `Class`/`isFraud`/`fraud_label`/kết quả `fraud_feedback`.
   Không dùng nhãn để tính tỷ lệ fraud theo merchant/thiết bị (target encoding) trừ khi tính **chỉ trên train** và theo thời gian.
2. **Không nhìn tương lai:** mọi cửa sổ chỉ dùng `event_time ≤ t`; cửa sổ lịch sử loại giao dịch hiện tại.
3. **Không dùng thông tin hậu giao dịch:** chargeback, kết quả điều tra, trạng thái xử lý sau, thời điểm đóng case.
4. **Fit trên train:** scaler, imputer, encoder, thống kê chuẩn hoá, ngưỡng chọn feature chỉ fit trên tập train.
5. **Khử trùng lặp trước khi chia tập:** trùng lặp rơi vào cả train lẫn test làm phình số liệu đánh giá.
6. **Xử lý mất cân bằng chỉ trên train:** `class_weight`, oversampling/SMOTE, undersampling **không** áp dụng cho validation/test.

### 7.2 Bảng rủi ro theo feature

| Feature | Rủi ro | Biện pháp |
|---|---|---|
| `tx_count_5m`, `amount_sum_1h`, `failed_tx_count_10m` | Cửa sổ vô tình lấy giao dịch sau `t` khi tính batch bằng `rowsBetween` không đúng | Dùng `rangeBetween(-W, 0)` theo `event_time`; unit test với chuỗi có thứ tự biết trước |
| `unique_cards_per_device_1h` | Như trên; thiết bị gắn với thẻ fraud về sau (nhãn lan truyền) | Chỉ dùng quan hệ thẻ–thiết bị quan sát được tại `t` |
| `amount_vs_avg_7d` | Trung bình gồm chính giao dịch hiện tại (làm tỷ lệ bị kéo về 1) hoặc gồm giao dịch tương lai | Cửa sổ `[t−7d, t)` loại hàng hiện tại; test với giao dịch đột biến |
| `is_new_merchant`, `is_new_country` | Profile dựng từ **toàn bộ** dữ liệu rồi join lại → "mới" bị sai | Profile phải dựng theo thời gian (as-of join) hoặc state tích luỹ |
| `hour_of_day` | Gần như không rủi ro; lưu ý múi giờ | Ghi rõ UTC/local |
| Target/frequency encoding (nếu thêm) | Rò rỉ nhãn | Chỉ fit trên train, theo thời gian |
| ULB `V1–V28` | PCA có thể được fit trên toàn bộ dữ liệu gốc bởi nhà phát hành (ngoài tầm kiểm soát) | Ghi vào giới hạn; không dùng để tuyên bố tổng quát hoá |

### 7.3 Nguyên tắc chia train / validation / test theo thời gian

1. **Chia theo thời gian, không xáo trộn:** train = giai đoạn sớm nhất, validation = tiếp theo, test = muộn nhất.
2. Tỷ lệ khởi điểm **60 / 20 / 20** theo thứ tự `Time`/`TransactionDT`/`event_time` (đúng cách `scripts/eda_ulb.py` đang chia và báo cáo số fraud mỗi tập).
3. **Khoảng đệm (gap)** giữa các tập bằng độ dài cửa sổ feature dài nhất (7 ngày với dữ liệu có đủ thời lượng) để không có feature ở validation/test dựa vào giao dịch thuộc train theo cách rò rỉ trạng thái.
4. **Một thẻ có thể xuất hiện ở nhiều tập** (điều bình thường trong thực tế); khi đó feature của giao dịch ở tập sau được phép dùng lịch sử từ tập trước vì đó là thông tin có thật tại thời điểm `t`.
5. **Chọn ngưỡng và siêu tham số trên validation**; test chỉ dùng **một lần** để báo cáo cuối.
6. Nếu một tập có ít hơn ~30 giao dịch fraud, báo cáo kèm khoảng tin cậy (bootstrap) hoặc dùng cross-validation theo thời gian (expanding window) thay cho một lần chia.
7. Không dùng k-fold ngẫu nhiên cho đánh giá chính (rò rỉ thời gian).

### 7.4 Mô phỏng "tại thời điểm t" khi tạo feature huấn luyện

Tạo feature huấn luyện bằng cùng logic window/profile như streaming: với mỗi giao dịch chỉ dùng dữ liệu có
`event_time ≤ t`. Kiểm thử bằng **test "truncate"**: tính feature của một giao dịch trên toàn bộ dữ liệu, rồi tính lại trên dữ
liệu bị cắt ở `t`; hai giá trị phải **bằng nhau**. Nếu khác ⇒ có rò rỉ tương lai.

## 8. Metrics đánh giá

| Metric | Công thức | Vì sao dùng |
|---|---|---|
| Precision | `TP / (TP + FP)` | Tỷ lệ alert đúng; ảnh hưởng chi phí review |
| Recall | `TP / (TP + FN)` | Tỷ lệ fraud bị bỏ lọt |
| F1-score | `2PR / (P + R)` | Cân bằng P/R ở một ngưỡng |
| PR-AUC | Diện tích dưới đường Precision–Recall | Phù hợp lớp fraud hiếm; baseline ngẫu nhiên = fraud rate |
| False Positive Rate | `FP / (FP + TN)` | Mức chặn/cảnh báo nhầm khách hàng thật |
| Throughput, Latency p50/p95 | events/s; ms từ ingest đến quyết định | Yêu cầu vận hành streaming |

**Vì sao không dùng accuracy làm metric duy nhất:** với fraud rate ≈ 0,17% (số liệu chính xác xem `eda_report.md`
mục 4), mô hình luôn đoán "không fraud" đạt accuracy > 99,8% nhưng Recall = 0. ROC-AUC cũng có thể lạc quan vì
TN rất lớn; ưu tiên PR-AUC. Báo cáo kèm Recall tại một FPR cố định (ví dụ FPR = 0,1%) để so sánh rule-based và ML.

**Điểm hoạt động (operating point):** chọn ngưỡng `REVIEW`/`BLOCK` để tối đa Recall với ràng buộc Precision hoặc FPR
trên **validation**; có thể bổ sung chi phí kỳ vọng `C_FN × FN + C_FP × FP` khi nhóm thống nhất được tỷ lệ chi phí.

## 9. Checklist nghiệm thu `feature_v1.md`

- [x] Mỗi feature có công thức, nguồn dữ liệu, key & cửa sổ, thời điểm tính, lý do nghiệp vụ (fraud pattern).
- [x] Có mục kiểm tra leakage theo từng feature và nguyên tắc chia theo thời gian.
- [x] Có cách chuyển thành Spark batch/streaming và lưu ý rủi ro.
- [x] Nêu rõ feature nào kiểm chứng được trên dataset nào (mục 4).
- [x] Nguyễn Trung Hiếu đã review tính khả thi với streaming schema (mục 6): schema v1 cung cấp đủ event-time, card/device/customer, amount, merchant, country và status cho toàn bộ feature v1.
- [x] Chốt phiên bản sau review ngày 2026-10-11: simulator hiện thực đúng năm scenario và `tests/test_scenarios.py` xác nhận các tín hiệu vượt ngưỡng khởi điểm.
