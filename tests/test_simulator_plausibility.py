
import json
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import check_simulator_plausibility as C  # noqa: E402

T0 = datetime(2026, 10, 4, 9, 0, 0, tzinfo=timezone.utc)
CFG = dict(C.DEFAULTS)


def ev(i, t, card="c1", dev="d1", amount=100000.0, status="success", **kw):
    e = {"transaction_id": f"tx_{i:06d}", "event_time": t.strftime("%Y-%m-%dT%H:%M:%SZ"),
         "customer_id": f"cus_{card}", "card_id_hash": f"sha256:{card}", "merchant_id": "merchant_0042",
         "amount": amount, "currency": "VND", "channel": "web", "device_id_hash": f"sha256:{dev}",
         "ip_hash": "sha256:ip1", "country": "VN", "transaction_status": status}
    e.update(kw)
    return e


def normal(n=300, seed=7):
    rnd = random.Random(seed)
    out, t = [], T0
    for i in range(n):
        t += timedelta(seconds=rnd.randint(40, 400))
        c = rnd.randint(1, 60)
        # mỗi thẻ gắn với thiết bị riêng, số tiền quanh một mức đặc trưng của thẻ
        out.append(ev(i, t, card=f"c{c}", dev=f"d{c}", amount=round(c * 2000 * rnd.uniform(0.7, 1.3), 0)))
    return out


def card_testing():
    return normal(150) + [ev(1000 + i, T0 + timedelta(hours=5, seconds=20 * i), card="victim", dev="dx",
                             amount=5000.0, status="failed") for i in range(6)]


def velocity():
    return normal(150) + [ev(2000 + i, T0 + timedelta(hours=5, seconds=30 * i), card="vel", dev="dv") for i in range(8)]


def device_sharing():
    return normal(150) + [ev(3000 + i, T0 + timedelta(hours=5, minutes=5 * i), card=f"shared{i}", dev="dshared")
                          for i in range(5)]


def high_value():
    base = [ev(4000 + i, T0 + timedelta(hours=5, minutes=30 * i), card="rich", dev="dr", amount=100000.0) for i in range(5)]
    return normal(150) + base + [ev(4100, T0 + timedelta(hours=9), card="rich", dev="dr", amount=9_000_000.0)]


def write(tmp_path, events, name="e.jsonl"):
    p = tmp_path / name
    p.write_text("\n".join(json.dumps(e) for e in events), encoding="utf-8")
    return p


def run(tmp_path, events, expect):
    p = write(tmp_path, events)
    return C.main(["--file", str(p), "--expect", expect])


def test_normal_passes_and_needs_100_events(tmp_path):
    assert run(tmp_path, normal(300), "normal") == 0
    assert run(tmp_path, normal(50), "normal") == 1  # < 100 event


@pytest.mark.parametrize("scenario,gen", [("card_testing", card_testing), ("velocity_fraud", velocity),
                                          ("device_sharing", device_sharing), ("high_value_anomaly", high_value)])
def test_each_fraud_pattern_detected_and_normal_does_not_pass_as_fraud(tmp_path, scenario, gen):
    assert run(tmp_path, gen(), scenario) == 0
    assert run(tmp_path, normal(300), scenario) == 1  # normal không được có tín hiệu fraud đó


def test_fraud_file_is_not_accepted_as_normal(tmp_path):
    assert run(tmp_path, velocity(), "normal") == 1


def test_event_classification_valid_invalid_duplicate_late():
    good = ev(1, T0)
    assert C.schema_problems(good) == []
    assert C.schema_problems({**good, "amount": -5})            # invalid: amount âm
    assert C.schema_problems({**good, "transaction_status": "pending"})
    assert C.schema_problems({k: v for k, v in good.items() if k != "card_id_hash"})
    assert C.schema_problems({**good, "event_time": "04/10/2026"})

    events = [ev(1, T0), ev(1, T0), ev(2, T0 + timedelta(minutes=10)), ev(3, T0 - timedelta(hours=2))]
    rep = C.analyse(events, 0, CFG)
    assert rep["duplicate_transaction_ids"] == 1                # duplicate cùng transaction_id
    assert rep["out_of_order_events"] == 1                      # late/out-of-order event
    assert rep["n_invalid"] == 0


def test_invalid_lines_and_bad_json_counted(tmp_path):
    p = tmp_path / "x.jsonl"
    p.write_text(json.dumps(ev(1, T0)) + "\n{not json}\n" + json.dumps({**ev(2, T0), "amount": 0}) + "\n")
    events, perr = C.load_events(p)
    rep = C.analyse(events, perr, CFG)
    assert perr == 1 and rep["n_invalid"] == 1 and rep["n_valid"] == 1


def test_missing_file_returns_2(tmp_path):
    assert C.main(["--file", str(tmp_path / "nope.jsonl")]) == 2
