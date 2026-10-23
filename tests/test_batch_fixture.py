import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "data-generator"))

from schema import classify_events  # noqa: E402

FIXTURE_DIR = ROOT / "tests" / "fixtures" / "batch" / "v1"


def read_jsonl(path):
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def test_batch_v1_fixture_matches_expected_classification_and_metrics():
    manifest = json.loads((FIXTURE_DIR / "manifest.json").read_text(encoding="utf-8"))
    expected = json.loads((FIXTURE_DIR / "expected_results.json").read_text(encoding="utf-8"))
    events = read_jsonl(FIXTURE_DIR / "transactions.jsonl")
    reference_time = datetime.fromisoformat(manifest["reference_processing_time_utc"].replace("Z", "+00:00"))

    assert len(events) == manifest["line_count"] == expected["input_count"]
    rows = classify_events(events, watermark_minutes=manifest["watermark_minutes"], now=reference_time)

    assert [row["classification"] for row in rows] == [
        "valid", "invalid", "duplicate", "late"
    ]
    counts = {name: sum(row["classification"] == name for row in rows)
              for name in ("valid", "invalid", "duplicate", "late")}
    assert counts == expected["counts"]
    assert sum(counts.values()) == expected["input_count"]
    assert "DQ-02" in rows[1]["issues"][0]
    assert "DQ-08" in rows[2]["issues"][0]
    assert "DQ-09" in rows[3]["issues"][0]
    assert expected["expected_output"]["cleaned_candidate_rows"] == counts["valid"]
    assert expected["expected_output"]["curated_rows_before_feature_enrichment"] == counts["valid"]


def test_batch_v1_fixture_is_synthetic_and_schema_complete():
    manifest = json.loads((FIXTURE_DIR / "manifest.json").read_text(encoding="utf-8"))
    events = read_jsonl(FIXTURE_DIR / "transactions.jsonl")
    required = {
        "transaction_id", "event_time", "customer_id", "card_id_hash", "merchant_id",
        "amount", "currency", "channel", "device_id_hash", "ip_hash", "country",
        "transaction_status",
    }
    assert manifest["synthetic_only"] is True
    assert manifest["contains_real_dataset_or_pii"] is False
    assert all(set(event) == required for event in events)
    assert all(event["event_time"].endswith("Z") for event in events)


def test_event_date_partition_is_derived_from_utc_event_time():
    expected = json.loads((FIXTURE_DIR / "expected_results.json").read_text(encoding="utf-8"))
    valid_event = read_jsonl(FIXTURE_DIR / "transactions.jsonl")[0]
    event_time = datetime.fromisoformat(valid_event["event_time"].replace("Z", "+00:00")).astimezone(timezone.utc)
    assert event_time.date().isoformat() in expected["expected_output"]["partition_event_date_utc"]
