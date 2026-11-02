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
        marker = f"{base}/manifests/batch_id={batch_id}/run_manifest.json"
        hadoop = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        if hadoop.exists(spark._jvm.org.apache.hadoop.fs.Path(marker)):
            raise RuntimeError(f"batch_id {batch_id} already exists; use a new immutable batch_id")
        if result["cleaned_rows"]:
            spark.createDataFrame(result["cleaned_rows"]).write.mode("errorifexists").partitionBy("event_date").parquet(f"{base}/cleaned/batch_id={batch_id}")
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
