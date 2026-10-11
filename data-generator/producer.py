#!/usr/bin/env python3
"""Produce deterministic FraudStream events to JSONL and, optionally, Kafka."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from scenarios import DEFAULT_START, SCENARIOS, generate, quality_case_events
from schema import json_line, parse_event_time, validate_event


def _write_jsonl(path: Path, events: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json_line(event) for event in events) + "\n", encoding="utf-8")


def _send_kafka(events: list[dict], bootstrap_server: str, topic: str) -> None:
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise RuntimeError("Thiếu kafka-python. Hãy cài `pip install -r requirements.txt`.") from exc
    producer = KafkaProducer(bootstrap_servers=bootstrap_server, value_serializer=lambda value: json_line(value).encode("utf-8"))
    try:
        for event in events:
            producer.send(topic, key=event["card_id_hash"].encode("utf-8"), value=event)
        producer.flush()
    finally:
        producer.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=SCENARIOS, default="normal")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--rate", type=float, default=20.0, help="Số event normal/giây")
    parser.add_argument("--duration", type=int, default=60, help="Thời lượng normal traffic, tính bằng giây")
    parser.add_argument("--start-time", default=DEFAULT_START.strftime("%Y-%m-%dT%H:%M:%SZ"))
    parser.add_argument("--output", type=Path, help="Đường dẫn JSONL local")
    parser.add_argument("--quality-output", type=Path, help="Ghi riêng file valid/invalid/duplicate/late cho test")
    parser.add_argument("--bootstrap-server", help="Kafka bootstrap server, ví dụ localhost:9092")
    parser.add_argument("--topic", default="transactions_raw")
    args = parser.parse_args(argv)

    try:
        start = parse_event_time(args.start_time)
        events = generate(args.scenario, rate=args.rate, duration=args.duration, seed=args.seed, start=start)
    except (TypeError, ValueError) as exc:
        parser.error(str(exc))
    bad = [event for event in events if validate_event(event)]
    if bad:
        raise RuntimeError(f"Simulator tạo {len(bad)} event không hợp lệ; dừng để bảo vệ data contract.")
    if args.output:
        _write_jsonl(args.output, events)
    else:
        for event in events:
            print(json_line(event))
    if args.quality_output:
        _write_jsonl(args.quality_output, quality_case_events(start))
    if args.bootstrap_server:
        _send_kafka(events, args.bootstrap_server, args.topic)
    destinations = []
    if args.output:
        destinations.append(str(args.output))
    if args.bootstrap_server:
        destinations.append(f"Kafka:{args.topic}")
    print(f"Generated {len(events)} valid {args.scenario} events -> {', '.join(destinations) or 'stdout'}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
