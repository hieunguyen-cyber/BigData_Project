"""Unit tests for Spark Batch data processing logic."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "spark-batch") not in sys.path:
    sys.path.insert(0, str(ROOT / "spark-batch"))

from batch_job import process_batch
FIXTURE_PATH = ROOT / "tests" / "fixtures" / "batch" / "v1" / "transactions.jsonl"
MANIFEST_PATH = ROOT / "tests" / "fixtures" / "batch" / "v1" / "manifest.json"


def test_process_batch_reconciles_and_classifies_correctly():
    raw_bytes = FIXTURE_PATH.read_bytes()
    raw_lines = raw_bytes.decode("utf-8").splitlines()
    manifest_info = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))

    ref_time = datetime.fromisoformat(
        manifest_info["reference_processing_time_utc"].replace("Z", "+00:00")
    ).astimezone(timezone.utc)

    result = process_batch(
        raw_lines=raw_lines,
        raw_bytes=raw_bytes,
        batch_id="test_batch_001",
        run_id="run_001",
        reference_time=ref_time,
        watermark_minutes=manifest_info["watermark_minutes"],
    )

    metrics = result["manifest"]["metrics"]
    assert metrics["input_records"] == 4
    assert metrics["valid_records"] == 1
    assert metrics["invalid_records"] == 1
    assert metrics["duplicate_records"] == 1
    assert metrics["late_records"] == 1
    assert metrics["cleaned_records"] == 1
    assert metrics["distinct_transaction_ids"] == 1
    assert metrics["classification_reconciliation"] is True

    # Check partition derivation
    assert result["cleaned_rows"][0]["event_date"] == "2026-10-04"
    assert result["manifest"]["partitions"] == ["2026-10-04"]
