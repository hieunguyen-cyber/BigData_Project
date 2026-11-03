# Checklist hoàn tất roadmap FraudStream

Tài liệu này thay thế phạm vi tạm thời của W4. Các hạng mục roadmap ban đầu đã
được đóng nghiệm thu trên codebase hiện tại.

| Hạng mục roadmap | Hiện thực | Kiểm chứng |
|---|---|---|
| Raw → cleaned → curated | PySpark validation, routing, Parquet Snappy, feature contract `feature_v1`, partition date/hour | `make spark-test` |
| ML baseline | Spark MLlib Logistic Regression và Random Forest, chọn model bằng validation PR-AUC | `make spark-ml-test` |
| Streaming | Spark Structured Streaming đọc Kafka, checkpoint, event-time rule engine, alert topic | Kafka Spark smoke: input 1, lag 0, output micro-batch `_SUCCESS` |
| Serving | Cassandra `transaction_risk`, `fraud_alert`, `dashboard_recent_decision` | Kafka worker → Cassandra → dashboard |
| Monitoring | Prometheus exporter/scoring metrics và Grafana overview | Hai targets healthy, decision metrics truy vấn được |
| Product UI | Flask operations dashboard đọc Cassandra read model | Browser E2E hiển thị `REVIEW` alert |

Lệnh nghiệm thu chính nằm tại `Makefile`: `make reproduce`, `make test`,
`make spark-test`, `make spark-ml-test` và `make monitoring`.
