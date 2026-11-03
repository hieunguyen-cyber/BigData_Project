"""Executable PySpark batch job for FraudStream's raw-to-cleaned layer.

This job intentionally keeps the project contract in the existing validator,
while Spark owns the actual read/write and Parquet materialisation. It supports
local paths for tests and ``hdfs://`` paths when submitted to the cluster.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "data-generator"))
sys.path.insert(0, str(ROOT / "spark-batch"))
from batch_job import process_batch  # noqa: E402


def write_curated_features(spark, cleaned_rows: list[dict], base: str, batch_id: str) -> int:
    """Materialise the versioned feature contract in curated Parquet.

    Every aggregation is bounded by event time and excludes the current row,
    preventing the transaction itself (or future rows) from leaking into a
    feature.  The resulting dataset is partitioned by UTC date and hour.
    """
    if not cleaned_rows:
        return 0
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    df = (spark.createDataFrame(cleaned_rows)
          .withColumn("event_ts", F.to_timestamp("event_time"))
          .withColumn("event_epoch", F.col("event_ts").cast("long"))
          .withColumn("event_hour", F.date_format("event_ts", "HH")))
    card_5m = Window.partitionBy("card_id_hash").orderBy("event_epoch").rangeBetween(-300, -1)
    card_1h = Window.partitionBy("card_id_hash").orderBy("event_epoch").rangeBetween(-3600, -1)
    card_7d = Window.partitionBy("card_id_hash").orderBy("event_epoch").rangeBetween(-604800, -1)
    device_1h = Window.partitionBy("device_id_hash").orderBy("event_epoch").rangeBetween(-3600, -1)
    merchant_history = Window.partitionBy("customer_id", "merchant_id").orderBy("event_epoch").rangeBetween(Window.unboundedPreceding, -1)
    country_history = Window.partitionBy("customer_id", "country").orderBy("event_epoch").rangeBetween(Window.unboundedPreceding, -1)
    curated = (df
        .withColumn("tx_count_5m", F.coalesce(F.count("transaction_id").over(card_5m), F.lit(0)))
        .withColumn("amount_sum_1h", F.coalesce(F.sum("amount").over(card_1h), F.lit(0.0)))
        .withColumn("failed_tx_count_10m", F.coalesce(F.sum(F.when(F.col("transaction_status") == "failed", 1).otherwise(0)).over(Window.partitionBy("card_id_hash").orderBy("event_epoch").rangeBetween(-600, -1)), F.lit(0)))
        .withColumn("unique_cards_per_device_1h", F.coalesce(F.size(F.collect_set("card_id_hash").over(device_1h)), F.lit(0)))
        .withColumn("amount_avg_7d", F.avg("amount").over(card_7d))
        .withColumn("amount_vs_avg_7d", F.when(F.col("amount_avg_7d") > 0, F.col("amount") / F.col("amount_avg_7d")))
        .withColumn("is_new_merchant", F.when(F.coalesce(F.count("merchant_id").over(merchant_history), F.lit(0)) == 0, F.lit(True)).otherwise(F.lit(False)))
        .withColumn("is_new_country", F.when(F.coalesce(F.count("country").over(country_history), F.lit(0)) == 0, F.lit(True)).otherwise(F.lit(False)))
        .drop("event_ts", "event_epoch", "amount_avg_7d"))
    curated.write.mode("errorifexists").partitionBy("event_date", "event_hour").parquet(f"{base}/curated/batch_id={batch_id}")
    return curated.count()


def run_spark_batch(input_path: str, output_root: str, batch_id: str, reference_time: str) -> dict:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("fraudstream-batch").getOrCreate()
    try:
        # wholetext preserves source order, which is required for late-event semantics.
        content = spark.read.text(input_path).collect()
        lines = [row["value"] for row in content]
        raw = ("\n".join(lines) + "\n").encode("utf-8")
        result = process_batch(
            lines, raw, batch_id, run_id=f"spark-{batch_id}",
            reference_time=datetime.fromisoformat(reference_time.replace("Z", "+00:00")).astimezone(timezone.utc),
        )
        base = output_root.rstrip("/")
        # Spark's text writer creates a directory containing part files.  The
        # directory itself is the immutable commit marker, not a made-up JSON
        # filename inside it.
        marker = f"{base}/manifests/batch_id={batch_id}"
        hadoop = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        if hadoop.exists(spark._jvm.org.apache.hadoop.fs.Path(marker)):
            raise RuntimeError(f"batch_id {batch_id} already exists; use a new immutable batch_id")
        if result["cleaned_rows"]:
            spark.createDataFrame(result["cleaned_rows"]).write.mode("errorifexists").partitionBy("event_date").parquet(f"{base}/cleaned/batch_id={batch_id}")
        result["manifest"]["metrics"]["curated_records"] = write_curated_features(
            spark, result["cleaned_rows"], base, batch_id
        )
        result["manifest"]["feature_contract_version"] = "feature_v1"
        for route, rows in (("raw/invalid", result["invalid_rows"]), ("late_events", result["late_rows"]), ("raw/duplicate", result["duplicate_rows"])):
            if rows:
                spark.createDataFrame([(json.dumps(row, ensure_ascii=False),) for row in rows], ["value"]).write.mode("errorifexists").text(f"{base}/{route}/batch_id={batch_id}")
        spark.createDataFrame([(json.dumps(result["manifest"], ensure_ascii=False),)], ["value"]).write.mode("errorifexists").text(f"{base}/manifests/batch_id={batch_id}")
        return result["manifest"]
    finally:
        spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input-path", required=True); p.add_argument("--output-root", required=True)
    p.add_argument("--batch-id", required=True); p.add_argument("--reference-time", required=True)
    a = p.parse_args()
    print(json.dumps(run_spark_batch(a.input_path, a.output_root, a.batch_id, a.reference_time), ensure_ascii=False))
