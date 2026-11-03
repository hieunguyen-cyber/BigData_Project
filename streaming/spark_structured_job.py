"""Spark Structured Streaming runtime for FraudStream's speed layer.

The existing ``stream_job.py`` is a small Python adapter used by unit tests.
This module is the real Spark entry point: it continuously reads Kafka,
applies the shared schema/rule contract per micro-batch, persists scores, and
emits fraud alerts back to Kafka.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "streaming"), str(ROOT / "data-generator")]
from engine import StreamingEngine  # noqa: E402
from metrics import SPARK_BATCH_DURATION, SPARK_INPUT_RATE, SPARK_PROCESSED_RATE, SPARK_STATE_ROWS  # noqa: E402


def run(bootstrap_server: str, input_topic: str, alert_topic: str, checkpoint_path: str,
        output_path: str, trigger_seconds: int) -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_json, to_json, struct, to_timestamp
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType
    from pyspark.sql.streaming import StreamingQueryListener

    spark = SparkSession.builder.appName("fraudstream-structured-streaming").getOrCreate()
    engine = StreamingEngine()
    fields=["transaction_id","event_time","customer_id","card_id_hash","merchant_id","currency","channel","device_id_hash","ip_hash","country","transaction_status"]
    event_schema=StructType([StructField(name,StringType(),True) for name in fields[:5]]+[StructField("amount",DoubleType(),True)]+[StructField(name,StringType(),True) for name in fields[5:]])

    class MetricsListener(StreamingQueryListener):
        def onQueryStarted(self, event): pass
        def onQueryTerminated(self, event): pass
        def onQueryProgress(self, event):
            progress=event.progress
            SPARK_INPUT_RATE.set(float(progress.inputRowsPerSecond))
            SPARK_PROCESSED_RATE.set(float(progress.processedRowsPerSecond))
            SPARK_BATCH_DURATION.set(float(progress.durationMs.get("triggerExecution",0)))
            SPARK_STATE_ROWS.set(sum(int(x.get("numRowsTotal",0)) for x in progress.stateOperators))
    spark.streams.addListener(MetricsListener())

    def process_microbatch(frame, batch_id: int) -> None:
        # The stateful rule contract is shared with the reference engine. This
        # bounded micro-batch implementation is appropriate for the local demo;
        # future scale-out can replace it with Spark stateful operators without
        # changing topics, schema, checkpoint, or output contracts.
        events = [json.loads(row["value"]) for row in frame.selectExpr("CAST(value AS STRING) value").collect()]
        scored = [result for result in (engine.process(event) for event in events) if result["route"] == "scored"]
        if not scored:
            return
        output = spark.createDataFrame([(json.dumps(result, ensure_ascii=False),) for result in scored], ["value"])
        output.write.mode("append").text(f"{output_path.rstrip('/')}/batch_id={batch_id}")
        alerts = [result for result in scored if result["alert"]]
        if alerts:
            spark.createDataFrame(
                [(result["event"]["card_id_hash"], json.dumps(result, ensure_ascii=False)) for result in alerts],
                ["key", "value"],
            ).selectExpr("CAST(key AS BINARY)", "CAST(value AS BINARY)").write.format("kafka").option(
                "kafka.bootstrap.servers", bootstrap_server
            ).option("topic", alert_topic).save()

    raw = (spark.readStream.format("kafka")
             .option("kafka.bootstrap.servers", bootstrap_server)
             .option("subscribe", input_topic).option("startingOffsets", "latest").load()
             .selectExpr("CAST(value AS STRING) raw_value"))
    # Spark owns watermark + stateful deduplication; the explicit raw value is
    # retained so invalid records still reach the shared quality-rule engine.
    parsed=(raw.select("raw_value",from_json("raw_value",event_schema).alias("event"))
            .select("raw_value","event.*").withColumn("event_ts",to_timestamp("event_time"))
            .withWatermark("event_ts","10 minutes").dropDuplicates(["transaction_id"]))
    query = (parsed.select(col("raw_value").alias("value"))
             .writeStream.outputMode("append").foreachBatch(process_microbatch).option("checkpointLocation", checkpoint_path)
             .trigger(processingTime=f"{trigger_seconds} seconds").start())
    query.awaitTermination()


def main() -> None:
    parser = argparse.ArgumentParser(description="FraudStream Spark Structured Streaming speed layer")
    parser.add_argument("--bootstrap-server", default="kafka.fraudstream.svc.cluster.local:9092")
    parser.add_argument("--input-topic", default="transactions_raw")
    parser.add_argument("--alert-topic", default="fraud_alerts")
    parser.add_argument("--checkpoint-path", default="hdfs:///fraud-data/checkpoints/fraudstream-speed-v1")
    parser.add_argument("--output-path", default="hdfs:///fraud-data/scored")
    parser.add_argument("--trigger-seconds", type=int, default=10)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.dry_run:
        print(json.dumps(vars(args), sort_keys=True))
    else:
        run(**{key: value for key, value in vars(args).items() if key != "dry_run"})


if __name__ == "__main__":
    main()
