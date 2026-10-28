# Giới hạn Kỹ thuật và Phạm vi Tuần 4 (FraudStream Limitations)

> Tài liệu mô tả trung thực ranh giới phạm vi hoàn thành của Tuần 4 theo tinh thần kỹ thuật nghiêm túc, tránh việc tuyên bố vượt quá thực tế (overclaiming).

---

## 1. Những phần cố ý CHƯA thuộc phạm vi Tuần 4

Theo đúng kế hoạch tại [`docs/week_4/week_4_target.md`](week_4_target.md), các hạng mục sau **không nằm trong phạm vi nghiệm thu của Tuần 4**:

1. **Curated Feature Enrichment đầy đủ**:
   - Tầng `curated` ở tuần này chỉ phản chiếu schema cấu trúc của `cleaned`.
   - Chưa tính các đặc trưng hành vi phức tạp như `amount_vs_avg_7d`, `unique_cards_per_device_1h`, `failed_tx_count_10m`.
   - Các feature này sẽ được hiện thực kèm kiểm tra chống rò rỉ thời gian (anti-leakage) ở **Tuần 5**.

2. **Machine Learning Model Training & Evaluation**:
   - Chưa huấn luyện mô hình Logistic Regression hoặc Random Forest trên Spark MLlib.
   - Chưa có artifact mô hình lưu trong `/fraud-data/models/`.

3. **Spark Structured Streaming & Online Scoring**:
   - Chưa triển khai stream reader đọc liên tục từ topic `transactions_raw` vào Spark Streaming.
   - Chưa chấm điểm online theo thời gian thực hay phát alert vào topic `fraud_alerts`.

4. **Ghi kết quả vào Cassandra Serving Layer**:
   - Cassandra đã sẵn sàng về hạ tầng (keyspace, schema, persistence).
   - Tuy nhiên, Spark batch trong W4 chỉ ghi ra HDFS Parquet, chưa ghi điểm rủi ro vào Cassandra.

5. **Giám sát Prometheus / Grafana Dashboard**:
   - Giám sát vận hành và trực quan hóa thuộc lộ trình **Tuần 7**.

---

## 2. Các giới hạn vận hành hiện tại

1. **Thứ tự xử lý Late Events**:
   - Việc xác định `late_events` dựa trên thứ tự xuất hiện của dòng dữ liệu (arrival order) kết hợp với watermark 10 phút.
   - Batch pipeline không thực hiện sắp xếp toàn bộ dữ liệu theo `event_time` trước khi phân loại để tránh rò rỉ thông tin tương lai (future leakage).

2. **Cơ chế Idempotency khi Rerun**:
   - Nếu chạy lại cùng một `batch_id` với file raw có cùng mã băm SHA-256, pipeline xử lý an toàn (idempotent overwrite).
   - Nếu truyền cùng một `batch_id` nhưng nội dung file raw bị thay đổi (khác SHA-256), hệ thống coi đây là xung đột dữ liệu (data conflict) và yêu cầu sinh `batch_id` mới thay vì ghi đè âm thầm.

3. **Tài nguyên Kind Local**:
   - Môi trường Kind chạy trên máy cá nhân bị giới hạn bởi RAM (khuyên dùng 8–10GB cho Docker Desktop).
   - Cần tuân thủ cấu hình tài nguyên nhỏ (`requests.memory: 256Mi-512Mi`) cho các runner pod để tránh hiện tượng pod rơi vào trạng thái `Pending`.
