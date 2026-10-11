#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

REQUIRED = ["transaction_id", "event_time", "customer_id", "card_id_hash", "merchant_id", "amount",
            "currency", "channel", "device_id_hash", "ip_hash", "country", "transaction_status"]
VALID_STATUS = {"success", "failed"}
SIGNALS = {  # scenario -> tên tín hiệu cần xuất hiện
    "card_testing": "failed_10m",
    "velocity_fraud": "count_5m",
    "device_sharing": "cards_per_device_1h",
    "high_value_anomaly": "amount_ratio",
}
DEFAULTS = {"count_5m": 5, "failed_10m": 3, "cards_per_device_1h": 3, "amount_ratio": 5.0,
            "min_history": 3, "normal_tolerance": 0.01}


def parse_time(s) -> datetime:
    return datetime.fromisoformat(str(s).replace("Z", "+00:00")).astimezone(timezone.utc)


def load_events(path: Path):
    events, parse_errors = [], 0
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                parse_errors += 1
    return events, parse_errors


def schema_problems(e: dict) -> list[str]:
    errs = [f"thiếu {k}" for k in REQUIRED if e.get(k) in (None, "")]
    if errs:
        return errs
    if not isinstance(e["amount"], (int, float)) or isinstance(e["amount"], bool) or e["amount"] <= 0:
        errs.append("amount phải là số > 0")
    if e["transaction_status"] not in VALID_STATUS:
        errs.append("transaction_status ngoài {success, failed}")
    try:
        parse_time(e["event_time"])
    except ValueError:
        errs.append("event_time không phải ISO-8601")
    if not (isinstance(e["currency"], str) and len(e["currency"]) == 3 and e["currency"].isupper()):
        errs.append("currency không phải mã 3 chữ hoa")
    return errs


def _max_window_count(times: list[float], window: float) -> int:
    """Số sự kiện lớn nhất trong cửa sổ (t-window, t] (times đã sort)."""
    best, j = 0, 0
    for i, t in enumerate(times):
        while times[j] <= t - window:
            j += 1
        best = max(best, i - j + 1)
    return best


def compute_signals(events: list[dict], cfg: dict) -> dict:
    by_card = defaultdict(list)
    by_device = defaultdict(list)
    for e in events:
        t = parse_time(e["event_time"]).timestamp()
        by_card[e["card_id_hash"]].append((t, e))
        by_device[e["device_id_hash"]].append((t, e))
    flagged = {k: set() for k in SIGNALS.values()}
    max_vals = {k: 0.0 for k in SIGNALS.values()}

    for card, lst in by_card.items():
        lst.sort(key=lambda x: x[0])
        c5 = _max_window_count([t for t, _ in lst], 300)
        fails = _max_window_count([t for t, e in lst if e["transaction_status"] == "failed"], 600)
        ratio, hist = 0.0, []
        for _, e in lst:
            if len(hist) >= cfg["min_history"]:
                med = statistics.median(hist)
                if med > 0:
                    ratio = max(ratio, e["amount"] / med)
            hist.append(e["amount"])
        for name, val in (("count_5m", c5), ("failed_10m", fails), ("amount_ratio", ratio)):
            max_vals[name] = max(max_vals[name], val)
            if val >= cfg[name]:
                flagged[name].add(card)

    for dev, lst in by_device.items():
        lst.sort(key=lambda x: x[0])
        dq, cnt, best = deque(), Counter(), 0
        for t, e in lst:
            dq.append((t, e["card_id_hash"]))
            cnt[e["card_id_hash"]] += 1
            while dq[0][0] <= t - 3600:
                _, c = dq.popleft()
                cnt[c] -= 1
                if cnt[c] == 0:
                    del cnt[c]
            best = max(best, len(cnt))
        max_vals["cards_per_device_1h"] = max(max_vals["cards_per_device_1h"], best)
        if best >= cfg["cards_per_device_1h"]:
            flagged["cards_per_device_1h"].add(dev)

    return {"n_cards": len(by_card), "n_devices": len(by_device),
            "max": {k: round(v, 2) for k, v in max_vals.items()},
            "n_flagged": {k: len(v) for k, v in flagged.items()}}


def analyse(events: list[dict], parse_errors: int, cfg: dict) -> dict:
    bad = Counter()
    valid = []
    for e in events:
        probs = schema_problems(e)
        if probs:
            for p in probs:
                bad[p] += 1
        else:
            valid.append(e)
    ids = Counter(e.get("transaction_id") for e in events)
    dup_ids = sum(c - 1 for c in ids.values() if c > 1)
    out_of_order = 0
    prev = None
    for e in valid:
        t = parse_time(e["event_time"])
        if prev and t < prev:
            out_of_order += 1
        prev = max(prev, t) if prev else t
    amounts = [e["amount"] for e in valid]
    rep = {
        "n_lines_parsed": len(events), "json_parse_errors": parse_errors,
        "n_valid": len(valid), "n_invalid": len(events) - len(valid), "invalid_reasons": dict(bad),
        "duplicate_transaction_ids": dup_ids, "out_of_order_events": out_of_order,
        "status_dist": dict(Counter(e["transaction_status"] for e in valid)),
        "channel_dist": dict(Counter(e["channel"] for e in valid)),
        "currency_dist": dict(Counter(e["currency"] for e in valid)),
        "country_dist": dict(Counter(e["country"] for e in valid).most_common(10)),
    }
    if amounts:
        qs = statistics.quantiles(amounts, n=100) if len(amounts) >= 2 else [amounts[0]] * 99
        rep["amount"] = {"median": statistics.median(amounts), "p95": qs[94], "p99": qs[98], "max": max(amounts)}
        ts = sorted(parse_time(e["event_time"]).timestamp() for e in valid)
        span = ts[-1] - ts[0]
        rep["time_span_s"] = span
        rep["events_per_s"] = round(len(valid) / span, 3) if span > 0 else None
        rep["signals"] = compute_signals(valid, cfg)
    return rep


def verdict(rep: dict, expect: str, cfg: dict) -> tuple[bool, list[str]]:
    notes = []
    if rep["n_valid"] == 0:
        return False, ["Không có event hợp lệ nào."]
    ok = True
    sig = rep["signals"]
    n_cards = max(sig["n_cards"], 1)
    if expect == "normal":
        for name, n in sig["n_flagged"].items():
            denom = sig["n_devices"] if name == "cards_per_device_1h" else n_cards
            frac = n / max(denom, 1)
            if frac > cfg["normal_tolerance"]:
                ok = False
                notes.append(f"Normal traffic nhưng {frac:.1%} thực thể vượt ngưỡng {name} (cho phép ≤ {cfg['normal_tolerance']:.0%}).")
        if rep["n_valid"] < 100:
            ok = False
            notes.append(f"Cần tối thiểu 100 event normal hợp lệ, hiện có {rep['n_valid']}.")
        if rep["duplicate_transaction_ids"]:
            ok = False
            notes.append("Normal traffic không được có transaction_id trùng.")
    elif expect in SIGNALS:
        name = SIGNALS[expect]
        if sig["n_flagged"][name] == 0:
            ok = False
            notes.append(f"Scenario {expect} nhưng không thực thể nào vượt ngưỡng {name} "
                         f"(max đo được = {sig['max'][name]}, ngưỡng = {cfg[name]}).")
    elif expect == "any":
        if sum(sig["n_flagged"].values()) == 0:
            ok = False
            notes.append("File 'mixed' nhưng không có tín hiệu fraud nào.")
    if rep["n_invalid"] and expect != "any":
        notes.append(f"Có {rep['n_invalid']} event vi phạm schema (có thể chủ đích nếu đây là file test invalid).")
    return ok, notes


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--file", type=Path, required=True)
    ap.add_argument("--expect", default="any",
                    choices=["normal", "card_testing", "velocity_fraud", "device_sharing", "high_value_anomaly", "any"])
    ap.add_argument("--json", type=Path, default=None, help="ghi báo cáo JSON")
    for k, v in DEFAULTS.items():
        ap.add_argument(f"--{k.replace('_', '-')}", type=type(v), default=v)
    a = ap.parse_args(argv)
    cfg = {k: getattr(a, k) for k in DEFAULTS}
    try:
        events, perr = load_events(a.file)
    except OSError as ex:
        print(f"[LỖI] {ex}", file=sys.stderr)
        return 2
    rep = analyse(events, perr, cfg)
    ok, notes = verdict(rep, a.expect, cfg) if "signals" in rep else (False, ["Không có event hợp lệ nào."])
    rep["expect"], rep["passed"], rep["notes"] = a.expect, ok, notes
    print(json.dumps(rep, ensure_ascii=False, indent=2))
    if a.json:
        a.json.write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[{'PASS' if ok else 'FAIL'}] expect={a.expect}", *notes, sep="\n  - ")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
