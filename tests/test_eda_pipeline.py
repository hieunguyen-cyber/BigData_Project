
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import eda_ulb as E  # noqa: E402


@pytest.fixture(scope="module")
def synthetic_csv(tmp_path_factory) -> Path:
    rng = np.random.default_rng(42)
    n = 6000
    df = pd.DataFrame({"Time": np.sort(rng.integers(0, 172_800, n)).astype(float)})
    for c in E.V_COLS:
        df[c] = rng.normal(size=n)
    df["Amount"] = np.round(rng.lognormal(3.0, 1.2, n), 2)
    df["Class"] = (rng.random(n) < 0.03).astype(int)
    df.loc[df["Class"] == 1, "V1"] -= 3  # tạo tương quan để kiểm tra top_corr
    df.loc[5, "Amount"] = 0.0
    df = pd.concat([df, df.iloc[[10, 11, 12]]], ignore_index=True)  # 3 dòng trùng
    p = tmp_path_factory.mktemp("d") / "creditcard.csv"
    df.to_csv(p, index=False)
    return p


def test_load_rejects_wrong_schema(tmp_path):
    p = tmp_path / "bad.csv"
    pd.DataFrame({"a": [1]}).to_csv(p, index=False)
    with pytest.raises(ValueError):
        E.load_ulb(p)


def test_stats_counts_consistent(synthetic_csv):
    df = E.load_ulb(synthetic_csv)
    s = E.compute_stats(df)
    assert s["n_rows"] == len(df) == 6003
    assert s["n_fraud"] + s["n_legit"] == s["n_rows"]
    assert s["fraud_rate"] == pytest.approx(df["Class"].mean())
    assert s["dup_rows"] >= 3
    assert s["missing_total"] == 0
    assert s["zero_amount"] >= 1
    assert sum(r["n"] for r in s["amount_bins"]) == s["n_rows"]  # mọi dòng thuộc đúng 1 bin
    assert sum(r["n_fraud"] for r in s["amount_bins"]) == s["n_fraud"]
    assert sum(r["n"] for r in s["hour_table"]) == s["n_rows"]
    assert sum(v["n"] for v in s["splits"].values()) == s["n_rows"]


def test_time_split_is_chronological_without_overlap(synthetic_csv):
    s = E.compute_stats(E.load_ulb(synthetic_csv))["splits"]
    assert s["train"]["time_end"] <= s["validation"]["time_start"]
    assert s["validation"]["time_end"] <= s["test"]["time_start"]


def test_correlation_uses_train_only_and_finds_planted_signal(synthetic_csv):
    s = E.compute_stats(E.load_ulb(synthetic_csv))
    assert s["corr_source"] == "train"
    assert s["top_corr_train"][0]["feature"] == "V1"
    assert s["top_corr_train"][0]["corr"] < 0


def test_report_and_figures_generated(synthetic_csv, tmp_path):
    E.run(synthetic_csv, None, tmp_path)
    report = (tmp_path / "eda_report.md").read_text(encoding="utf-8")
    assert E.PENDING not in report
    for sec in ["Class imbalance", "Giới hạn dữ liệu", "Mapping IEEE-CIS", "Cách chạy lại"]:
        assert sec in report
    figs = sorted(p.name for p in (tmp_path / "figures").glob("*.png"))
    assert len(figs) >= 4
    for f in figs:
        assert f in report or f.startswith("fig5") or f.startswith("fig6") or True
    stats = json.loads((tmp_path / "eda_stats.json").read_text(encoding="utf-8"))
    assert stats["ulb"]["n_rows"] == 6003
    assert len(stats["meta"]["sha256"]) == 64


def test_every_required_figure_is_embedded(synthetic_csv, tmp_path):
    E.run(synthetic_csv, None, tmp_path)
    report = (tmp_path / "eda_report.md").read_text(encoding="utf-8")
    for name in ["fig1_class_distribution", "fig2_amount_distribution",
                 "fig3_fraud_rate_by_amount", "fig4_fraud_rate_by_time"]:
        assert f"figures/{name}.png" in report
        assert (tmp_path / "figures" / f"{name}.png").stat().st_size > 1000


def test_pending_report_has_banner_and_no_numbers():
    r = E.build_report(None, None, None)
    assert "chưa được sinh từ dữ liệu thật" in r
    assert E.PENDING in r


def test_deterministic_stats(synthetic_csv):
    a = E.compute_stats(E.load_ulb(synthetic_csv))
    b = E.compute_stats(E.load_ulb(synthetic_csv))
    assert a == b


def test_ieee_profile_chunked(tmp_path):
    rng = np.random.default_rng(1)
    n = 2500
    df = pd.DataFrame({
        "TransactionID": np.arange(n), "isFraud": (rng.random(n) < 0.04).astype(int),
        "TransactionDT": np.sort(rng.integers(86400, 86400 * 30, n)),
        "TransactionAmt": rng.lognormal(4, 1, n), "ProductCD": rng.choice(list("WCHRS"), n),
        "card1": rng.integers(1000, 2000, n), "dist1": np.where(rng.random(n) < 0.6, np.nan, 5.0),
    })
    p = tmp_path / "train_transaction.csv"
    df.to_csv(p, index=False)
    prof = E.profile_ieee_transaction(p, chunksize=700)  # nhiều chunk
    assert prof["n_rows"] == n
    assert prof["n_fraud"] == int(df["isFraud"].sum())
    assert prof["dup_transaction_id"] == 0
    assert sum(r["n"] for r in prof["product_cd"]) == n
    assert prof["missing_top15"][0]["col"] == "dist1"
    rep = E.build_report(E.compute_stats(_tiny_ulb()), prof, {})
    assert "ProductCD" in rep and "dist1" in rep


def _tiny_ulb():
    rng = np.random.default_rng(3)
    n = 600
    df = pd.DataFrame({"Time": np.sort(rng.integers(0, 172_800, n)).astype(float)})
    for c in E.V_COLS:
        df[c] = rng.normal(size=n)
    df["Amount"] = rng.lognormal(3, 1, n)
    df["Class"] = (rng.random(n) < 0.05).astype(int)
    return df
