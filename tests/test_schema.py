import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "data-generator"))

from scenarios import DEFAULT_START, quality_case_events
from schema import classify_events, synthetic_hash, validate_event


def test_synthetic_hash_is_stable_and_non_sensitive():
    assert synthetic_hash("card-1") == synthetic_hash("card-1")
    assert synthetic_hash("card-1").startswith("sha256:")
    assert len(synthetic_hash("card-1")) == 71


def test_quality_cases_follow_the_project_classification_order():
    rows = classify_events(quality_case_events(), now=DEFAULT_START + timedelta(minutes=1))
    assert [row["classification"] for row in rows] == ["valid", "invalid", "duplicate", "late"]
    assert "DQ-02" in rows[1]["issues"][0]
    assert "DQ-08" in rows[2]["issues"][0]
    assert "DQ-09" in rows[3]["issues"][0]


def test_validator_rejects_real_or_malformed_sensitive_identifiers():
    event = quality_case_events()[0]
    broken = {**event, "card_id_hash": "4111111111111111"}
    assert any(issue.startswith("DQ-10") for issue in validate_event(broken))
