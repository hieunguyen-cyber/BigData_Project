"""Deterministic, non-sensitive transaction scenarios for FraudStream."""

from __future__ import annotations

import math
import random
from datetime import datetime, timedelta, timezone
from typing import Any

from schema import synthetic_hash

DEFAULT_START = datetime(2026, 10, 4, 9, 0, 0, tzinfo=timezone.utc)
SCENARIOS = ("normal", "card_testing", "velocity_fraud", "device_sharing", "high_value_anomaly")


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _event(
    sequence: int,
    event_time: datetime,
    *,
    card: str,
    device: str,
    amount: float,
    status: str = "success",
    country: str = "VN",
    channel: str = "web",
    merchant: str = "merchant_0042",
) -> dict[str, Any]:
    return {
        "transaction_id": f"tx_{sequence:08d}",
        "event_time": _iso(event_time),
        "customer_id": f"cus_{card}",
        "card_id_hash": synthetic_hash(f"card:{card}"),
        "merchant_id": merchant,
        "amount": round(float(amount), 2),
        "currency": "VND",
        "channel": channel,
        "device_id_hash": synthetic_hash(f"device:{device}"),
        "ip_hash": synthetic_hash(f"ip:{device}"),
        "country": country,
        "transaction_status": status,
    }


def normal_events(
    count: int,
    *,
    rate: float,
    seed: int,
    start: datetime = DEFAULT_START,
) -> list[dict[str, Any]]:
    """Create normal traffic with no card/device meeting fraud thresholds.

    Normal cards are unique during a run. This deliberately makes the control
    stream a clean baseline for Week 1 feature and plausibility tests.
    """
    if count < 1 or rate <= 0:
        raise ValueError("count và rate phải lớn hơn 0")
    rng = random.Random(seed)
    events: list[dict[str, Any]] = []
    for index in range(count):
        moment = start + timedelta(seconds=index / rate)
        amount = min(rng.lognormvariate(math.log(180_000), 0.7), 3_500_000)
        events.append(
            _event(
                index + 1,
                moment,
                card=f"normal-card-{index:05d}",
                device=f"normal-device-{index:05d}",
                amount=amount,
                status="failed" if index % 61 == 0 else "success",
                country="SG" if index % 23 == 0 else "VN",
                channel=("web", "mobile", "api")[index % 3],
                merchant=f"merchant_{index % 25:04d}",
            )
        )
    return events


def generate(
    scenario: str,
    *,
    rate: float = 20.0,
    duration: int = 60,
    seed: int = 42,
    start: datetime = DEFAULT_START,
) -> list[dict[str, Any]]:
    """Generate normal traffic plus one explicit fraud pattern when requested."""
    if scenario not in SCENARIOS:
        raise ValueError(f"scenario phải thuộc {', '.join(SCENARIOS)}")
    count = max(100, math.ceil(rate * duration))
    events = normal_events(count, rate=rate, seed=seed, start=start)
    if scenario == "normal":
        return events

    next_sequence = len(events) + 1
    cursor = start + timedelta(seconds=count / rate + 60)
    injected: list[dict[str, Any]] = []

    if scenario == "card_testing":
        for index in range(6):
            injected.append(_event(next_sequence + index, cursor + timedelta(seconds=20 * index), card="card-test-victim", device="card-test-device", amount=5_000, status="failed", merchant="merchant_0099"))
    elif scenario == "velocity_fraud":
        for index in range(8):
            injected.append(_event(next_sequence + index, cursor + timedelta(seconds=30 * index), card="velocity-victim", device="velocity-device", amount=150_000, merchant="merchant_0077"))
    elif scenario == "device_sharing":
        for index in range(5):
            injected.append(_event(next_sequence + index, cursor + timedelta(minutes=5 * index), card=f"shared-card-{index}", device="shared-device", amount=180_000, merchant="merchant_0066"))
    elif scenario == "high_value_anomaly":
        for index in range(5):
            injected.append(_event(next_sequence + index, cursor + timedelta(minutes=30 * index), card="high-value-card", device="high-value-device", amount=100_000, merchant="merchant_0055"))
        injected.append(_event(next_sequence + 5, cursor + timedelta(hours=3), card="high-value-card", device="high-value-device", amount=900_000, merchant="merchant_0055"))
    return events + injected


def quality_case_events(start: datetime = DEFAULT_START) -> list[dict[str, Any]]:
    """Return valid, invalid, duplicate, and late records for data-quality tests."""
    valid = _event(90_000_001, start, card="quality-card", device="quality-device", amount=120_000)
    invalid = {**_event(90_000_002, start + timedelta(seconds=5), card="invalid-card", device="invalid-device", amount=-1), "transaction_status": "pending"}
    duplicate = dict(valid)
    late = _event(90_000_003, start - timedelta(hours=2), card="late-card", device="late-device", amount=80_000)
    return [valid, invalid, duplicate, late]
