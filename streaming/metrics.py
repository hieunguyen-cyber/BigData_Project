"""Prometheus metrics shared by the online scoring path and UI."""
from prometheus_client import Counter, Histogram, Gauge
EVENTS=Counter("fraudstream_events_total","Processed events",["route"])
DECISIONS=Counter("fraudstream_decisions_total","Fraud decisions",["decision"])
LATENCY=Histogram("fraudstream_event_processing_seconds","Per-event scoring latency")
KAFKA_LAG=Gauge("fraudstream_kafka_consumer_lag","Current scoring consumer lag")
SPARK_INPUT_RATE=Gauge("fraudstream_spark_input_rows_per_second","Spark Structured Streaming input rate")
SPARK_PROCESSED_RATE=Gauge("fraudstream_spark_processed_rows_per_second","Spark Structured Streaming processed rate")
SPARK_BATCH_DURATION=Gauge("fraudstream_spark_batch_duration_ms","Spark Structured Streaming batch duration")
SPARK_STATE_ROWS=Gauge("fraudstream_spark_state_rows","Spark Structured Streaming state rows")
