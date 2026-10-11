"""Data contract and quality classification for FraudStream transaction events."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

REQUIRED_FIELDS = (
    "transaction_id",
    "event_time",
    "customer_id",
    "card_id_hash",
    "merchant_id",
    "amount",
    "currency",
    "channel",
    "device_id_hash",
    "ip_hash",
    "country",
    "transaction_status",
)
VALID_STATUSES = {"success", "failed"}
VALID_CHANNELS = {"web", "mobile", "api"}
HASH_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
COUNTRY_RE = re.compile(r"^[A-Z]{2}$")
CURRENCY_RE = re.compile(r"^[A-Z]{3}$")


def parse_event_time(value: str) -> datetime:
    """Parse an ISO-8601 timestamp and normalize it to UTC."""
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("event_time phải có timezone UTC")
    return parsed.astimezone(timezone.utc)


def synthetic_hash(value: str) -> str:
    """Create a deterministic, non-sensitive identifier for simulator records."""
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def validate_event(event: dict[str, Any], now: datetime | None = None) -> list[str]:
    """Return data-quality rule codes violated by one transaction event."""
    issues: list[str] = []
    missing = [field for field in REQUIRED_FIELDS if event.get(field) in (None, "")]
    if missing:
        return ["DQ-01: thiếu " + ", ".join(missing)]

    amount = event["amount"]
    if isinstance(amount, bool) or not isinstance(amount, (int, float)) or amount <= 0:
        issues.append("DQ-02: amount phải là số lớn hơn 0")
    if event["transaction_status"] not in VALID_STATUSES:
        issues.append("DQ-03: transaction_status không hợp lệ")
    try:
        event_time = parse_event_time(event["event_time"])
        if now is not None and event_time > now + timedelta(minutes=5):
            issues.append("DQ-05: event_time vượt quá thời điểm hiện tại 5 phút")
    except (TypeError, ValueError):
        issues.append("DQ-04: event_time không phải ISO-8601 UTC hợp lệ")
    if not isinstance(event["currency"], str) or not CURRENCY_RE.fullmatch(event["currency"]):
        issues.append("DQ-06: currency phải là mã ISO 4217 ba chữ in hoa")
    if not isinstance(event["country"], str) or not COUNTRY_RE.fullmatch(event["country"]):
        issues.append("DQ-06: country phải là mã ISO 3166-1 alpha-2")
    if event["channel"] not in VALID_CHANNELS:
        issues.append("DQ-01: channel không thuộc web/mobile/api")
    for field in ("card_id_hash", "device_id_hash", "ip_hash"):
        if not isinstance(event[field], str) or not HASH_RE.fullmatch(event[field]):
            issues.append(f"DQ-10: {field} phải là hash SHA-256 synthetic")
    return issues


def classify_events(
    events: Iterable[dict[str, Any]],
    *,
    watermark_minutes: int = 10,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Classify each event exactly once: invalid, duplicate, late, or valid.

    Classification order follows the project contract: schema/constraint, duplicate,
    then late event. A late event remains valid structurally but belongs in the
    `late_events` path instead of the online stateful pipeline.
    """
    seen_ids: set[str] = set()
    max_event_time: datetime | None = None
    rows: list[dict[str, Any]] = []
    watermark = timedelta(minutes=watermark_minutes)

    for event in events:
        issues = validate_event(event, now=now)
        if issues:
            rows.append({"classification": "invalid", "issues": issues, "event": event})
            continue
        transaction_id = event["transaction_id"]
        if transaction_id in seen_ids:
            rows.append({"classification": "duplicate", "issues": ["DQ-08: transaction_id đã xuất hiện"], "event": event})
            continue
        seen_ids.add(transaction_id)
        event_time = parse_event_time(event["event_time"])
        if max_event_time is not None and event_time < max_event_time - watermark:
            rows.append({"classification": "late", "issues": ["DQ-09: event_time trễ hơn watermark"], "event": event})
            continue
        max_event_time = max(max_event_time, event_time) if max_event_time else event_time
        rows.append({"classification": "valid", "issues": [], "event": event})
    return rows


def json_line(event: dict[str, Any]) -> str:
    """Serialize events consistently for Kafka values and JSONL outputs."""
    return json.dumps(event, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
