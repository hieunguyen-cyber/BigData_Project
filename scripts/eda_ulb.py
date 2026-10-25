
from __future__ import annotations

import argparse
import hashlib
import json
import platform
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
ROOT = Path(__file__).resolve().parents[1]
V_COLS = [f"V{i}" for i in range(1, 29)]
EXPECTED_COLS = ["Time", *V_COLS, "Amount", "Class"]
AMOUNT_BINS = [0, 1, 10, 50, 100, 500, 1000, np.inf]
AMOUNT_LABELS = ["0–1", "1–10", "10–50", "50–100", "100–500", "500–1000", ">1000"]
SPLIT_FRACTIONS = (0.6, 0.2, 0.2)  # train / validation / test theo thời gian
PENDING = "⟨chưa chạy⟩"
def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_ulb(path: Path | str) -> pd.DataFrame:
    df = pd.read_csv(path)
    missing = [c for c in EXPECTED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"File không đúng schema ULB, thiếu cột: {missing}")
    df = df[EXPECTED_COLS].copy()
    return df.sort_values("Time", kind="stable").reset_index(drop=True)


def _describe(s: pd.Series) -> dict:
    q = s.quantile([0.25, 0.5, 0.75, 0.95, 0.99])
    return {
        "count": int(s.count()), "mean": float(s.mean()), "std": float(s.std()),
        "min": float(s.min()), "p25": float(q[0.25]), "median": float(q[0.5]),
        "p75": float(q[0.75]), "p95": float(q[0.95]), "p99": float(q[0.99]),
        "max": float(s.max()), "skew": float(s.skew()),
    }


def _rate_table(keys: pd.Series, y: pd.Series) -> list[dict]:
    g = pd.DataFrame({"k": keys, "y": y}).groupby("k", observed=False)["y"].agg(["size", "sum"])
    out = []
    for k, row in g.iterrows():
        n, f = int(row["size"]), int(row["sum"])
        out.append({"key": str(k), "n": n, "n_fraud": f, "rate": (f / n) if n else None})
    return out


def compute_stats(df: pd.DataFrame) -> dict:
    n = len(df)
    y = df["Class"]
    n_fraud = int(y.sum())
    n_legit = n - n_fraud
    miss = df.isna().sum()
    dup_mask = df.duplicated()
    amt = df["Amount"]

    amount_bin = pd.cut(amt, bins=AMOUNT_BINS, labels=AMOUNT_LABELS, include_lowest=True)
    hour = ((df["Time"] // 3600) % 24).astype(int)

    # chia theo thời gian (df đã sort theo Time)
    i1 = int(n * SPLIT_FRACTIONS[0])
    i2 = int(n * (SPLIT_FRACTIONS[0] + SPLIT_FRACTIONS[1]))
    parts = {"train": df.iloc[:i1], "validation": df.iloc[i1:i2], "test": df.iloc[i2:]}
    splits = {}
    for name, p in parts.items():
        splits[name] = {
            "n": int(len(p)), "n_fraud": int(p["Class"].sum()),
            "rate": float(p["Class"].mean()) if len(p) else None,
            "time_start": float(p["Time"].min()) if len(p) else None,
            "time_end": float(p["Time"].max()) if len(p) else None,
        }

    # tương quan chỉ tính trên TRAIN để tránh leakage khi chọn feature
    train = parts["train"]
    cols = [*V_COLS, "Amount"]
    corr = train[cols].corrwith(train["Class"]).dropna()
    top = corr.reindex(corr.abs().sort_values(ascending=False).index).head(10)

    return {
        "n_rows": n, "n_cols": int(df.shape[1]),
        "n_fraud": n_fraud, "n_legit": n_legit,
        "fraud_rate": n_fraud / n,
        "imbalance_ratio": (n_legit / n_fraud) if n_fraud else None,
        "majority_accuracy": n_legit / n,
        "missing_total": int(miss.sum()),
        "missing_by_col": {k: int(v) for k, v in miss.items() if v > 0},
        "dup_rows": int(dup_mask.sum()),
        "dup_rows_fraud": int((dup_mask & (y == 1)).sum()),
        "time_min": float(df["Time"].min()), "time_max": float(df["Time"].max()),
        "time_span_hours": float((df["Time"].max() - df["Time"].min()) / 3600),
        "amount_all": _describe(amt),
        "amount_legit": _describe(amt[y == 0]),
        "amount_fraud": _describe(amt[y == 1]) if n_fraud else None,
        "zero_amount": int((amt == 0).sum()),
        "zero_amount_fraud": int(((amt == 0) & (y == 1)).sum()),
        "amount_bins": _rate_table(amount_bin, y),
        "hour_table": _rate_table(hour, y),
        "splits": splits,
        "top_corr_train": [{"feature": k, "corr": float(v)} for k, v in top.items()],
        "corr_source": "train",
    }


# --------------------------------------------------------------------------- #
# IEEE-CIS (tuỳ chọn): profile theo chunk để không tốn RAM
# --------------------------------------------------------------------------- #


def profile_ieee_transaction(path: Path | str, chunksize: int = 100_000) -> dict:
    n = fraud = 0
    miss: pd.Series | None = None
    ids: list[np.ndarray] = []
    dt_min, dt_max = np.inf, -np.inf
    amounts = []
    prod: dict[str, list[int]] = {}
    ncols = 0
    for ch in pd.read_csv(path, chunksize=chunksize):
        ncols = ch.shape[1]
        n += len(ch)
        fraud += int(ch["isFraud"].sum())
        m = ch.isna().sum()
        miss = m if miss is None else miss.add(m, fill_value=0)
        ids.append(ch["TransactionID"].to_numpy())
        dt_min, dt_max = min(dt_min, ch["TransactionDT"].min()), max(dt_max, ch["TransactionDT"].max())
        amounts.append(ch["TransactionAmt"].to_numpy())
        for k, g in ch.groupby("ProductCD")["isFraud"]:
            a = prod.setdefault(str(k), [0, 0])
            a[0] += len(g)
            a[1] += int(g.sum())
    all_ids = np.concatenate(ids)
    amt = pd.Series(np.concatenate(amounts))
    miss_pct = (miss / n * 100).sort_values(ascending=False)
    return {
        "n_rows": n, "n_cols": ncols, "n_fraud": fraud, "fraud_rate": fraud / n,
        "dup_transaction_id": int(n - len(np.unique(all_ids))),
        "dt_span_days": float((dt_max - dt_min) / 86400),
        "amount": _describe(amt),
        "missing_top15": [{"col": k, "pct": float(v)} for k, v in miss_pct.head(15).items()],
        "cols_over_50pct_missing": int((miss_pct > 50).sum()),
        "product_cd": [{"key": k, "n": v[0], "n_fraud": v[1], "rate": v[1] / v[0]}
                       for k, v in sorted(prod.items())],
    }


# --------------------------------------------------------------------------- #
# Biểu đồ (matplotlib thuần; mỗi hình có tiêu đề, nhãn trục, chú thích)
# --------------------------------------------------------------------------- #


def _save(fig, outdir: Path, name: str) -> Path:
    outdir.mkdir(parents=True, exist_ok=True)
    p = outdir / name
    fig.tight_layout()
    fig.savefig(p, dpi=140)
    plt.close(fig)
    return p


def plot_class_distribution(df, stats, outdir: Path) -> Path:
    fig, ax = plt.subplots(figsize=(6.5, 4.5))
    counts = [stats["n_legit"], stats["n_fraud"]]
    bars = ax.bar(["Non-fraud (Class=0)", "Fraud (Class=1)"], counts, color=["#4C78A8", "#E45756"])
    ax.set_yscale("log")
    ax.set_ylabel("Số giao dịch (thang log)")
    ax.set_xlabel("Nhãn giao dịch")
    ax.set_title("Hình 1. Phân bố fraud / non-fraud (ULB)")
    for b, c in zip(bars, counts):
        ax.annotate(f"{c:,}\n({c / stats['n_rows']:.3%})", (b.get_x() + b.get_width() / 2, max(c, 1)),
                    ha="center", va="bottom", fontsize=9)
    ax.set_ylim(top=max(max(counts), 1) * 8)
    ax.legend(bars, ["Non-fraud", "Fraud"], title="Chú thích", loc="upper right")
    return _save(fig, outdir, "fig1_class_distribution.png")


def plot_amount_distribution(df, stats, outdir: Path) -> Path:
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))
    cap = stats["amount_all"]["p99"]
    axes[0].hist(df["Amount"].clip(upper=cap), bins=60, color="#4C78A8", label="Toàn tập")
    axes[0].set_title(f"Amount (cắt tại p99 = {cap:,.0f})")
    axes[0].set_xlabel("Amount")
    axes[0].set_ylabel("Số giao dịch")
    axes[0].legend(title="Chú thích")
    for lab, name, col in [(0, "Non-fraud", "#4C78A8"), (1, "Fraud", "#E45756")]:
        s = np.log1p(df.loc[df["Class"] == lab, "Amount"])
        if len(s):
            axes[1].hist(s, bins=50, density=True, alpha=0.55, label=name, color=col)
    axes[1].set_title("log(1 + Amount) theo nhãn (chuẩn hoá mật độ)")
    axes[1].set_xlabel("log(1 + Amount)")
    axes[1].set_ylabel("Mật độ")
    axes[1].legend(title="Chú thích")
    fig.suptitle("Hình 2. Phân bố giá trị giao dịch")
    return _save(fig, outdir, "fig2_amount_distribution.png")


def plot_fraud_rate_by_amount(df, stats, outdir: Path) -> Path:
    t = stats["amount_bins"]
    rates = [(r["rate"] or 0) * 100 for r in t]
    fig, ax = plt.subplots(figsize=(8, 4.8))
    bars = ax.bar(AMOUNT_LABELS, rates, color="#F58518", label="Fraud rate theo nhóm")
    ax.axhline(stats["fraud_rate"] * 100, color="k", ls="--",
               label=f"Baseline toàn tập ({stats['fraud_rate']:.3%})")
    for b, r in zip(bars, t):
        ax.annotate(f"n={r['n']:,}\nfraud={r['n_fraud']}", (b.get_x() + b.get_width() / 2, b.get_height()),
                    ha="center", va="bottom", fontsize=8)
    ax.set_xlabel("Nhóm giá trị giao dịch (Amount)")
    ax.set_ylabel("Fraud rate (%)")
    ax.set_title("Hình 3. Fraud rate theo nhóm giá trị giao dịch")
    ax.legend(title="Chú thích")
    ax.set_ylim(top=(max(rates + [stats["fraud_rate"] * 100]) * 1.35) or 1)
    return _save(fig, outdir, "fig3_fraud_rate_by_amount.png")


def plot_fraud_rate_by_time(df, stats, outdir: Path) -> Path:
    t = stats["hour_table"]
    hours = [int(r["key"]) for r in t]
    rates = [(r["rate"] or 0) * 100 for r in t]
    fig, ax = plt.subplots(figsize=(10, 4.8))
    ax.bar(hours, rates, color="#E45756", label="Fraud rate theo giờ tương đối")
    ax.axhline(stats["fraud_rate"] * 100, color="k", ls="--", label="Baseline toàn tập")
    ax.set_xlabel("Giờ tương đối = (Time // 3600) mod 24  (Time: giây kể từ giao dịch đầu tiên)")
    ax.set_ylabel("Fraud rate (%)")
    ax.set_xticks(hours)
    ax.set_title("Hình 4. Fraud rate theo thời gian (giờ tương đối)")
    ax2 = ax.twinx()
    ax2.plot(hours, [r["n"] for r in t], color="#4C78A8", marker="o", ms=3, label="Số giao dịch")
    ax2.set_ylabel("Số giao dịch")
    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.set_ylim(top=(max(rates + [stats["fraud_rate"] * 100]) * 1.3) or 1)
    ax2.set_ylim(bottom=0, top=max(r["n"] for r in t) * 1.6)
    ax.legend(h1 + h2, l1 + l2, title="Chú thích", loc="upper center", ncol=3, fontsize=8)
    return _save(fig, outdir, "fig4_fraud_rate_by_time.png")


def plot_amount_boxplot(df, stats, outdir: Path) -> Path:
    fig, ax = plt.subplots(figsize=(6, 4.8))
    data = [np.log1p(df.loc[df["Class"] == c, "Amount"]) for c in (0, 1)]
    ax.boxplot(data)
    ax.set_xticks([1, 2])
    ax.set_xticklabels(["Non-fraud", "Fraud"])
    ax.set_ylabel("log(1 + Amount)")
    ax.set_xlabel("Nhãn giao dịch")
    ax.set_title("Hình 5. Boxplot log(1+Amount) theo nhãn")
    return _save(fig, outdir, "fig5_amount_boxplot.png")


def plot_top_corr(df, stats, outdir: Path) -> Path:
    t = stats["top_corr_train"][::-1]
    fig, ax = plt.subplots(figsize=(6.5, 4.8))
    ax.barh([r["feature"] for r in t], [r["corr"] for r in t],
            color=["#E45756" if r["corr"] > 0 else "#4C78A8" for r in t], label="Pearson r với Class")
    ax.axvline(0, color="k", lw=0.8)
    ax.set_xlabel("Hệ số tương quan Pearson với Class (chỉ trên tập train)")
    ax.set_ylabel("Feature")
    ax.set_title("Hình 6. Top 10 |tương quan| với nhãn fraud (train)")
    ax.legend(title="Chú thích")
    return _save(fig, outdir, "fig6_top_corr.png")


PLOTS = [plot_class_distribution, plot_amount_distribution, plot_fraud_rate_by_amount,
         plot_fraud_rate_by_time, plot_amount_boxplot, plot_top_corr]


def make_all_figures(df, stats, outdir: Path) -> list[Path]:
    return [p(df, stats, outdir) for p in PLOTS]


# --------------------------------------------------------------------------- #
# Nhận xét tự động (diễn giải bằng số liệu thật)
# --------------------------------------------------------------------------- #


def _pct(x, d=3):
    return f"{x * 100:.{d}f}%"


def comment_class(s):
    if s is None:
        return PENDING
    ratio = f"{s['imbalance_ratio']:,.0f} : 1" if s["imbalance_ratio"] else "n/a"
    return (f"Tập dữ liệu có **{s['n_rows']:,}** giao dịch, trong đó **{s['n_fraud']:,}** fraud "
            f"(**{_pct(s['fraud_rate'])}**); tỷ lệ non-fraud : fraud ≈ **{ratio}**. "
            f"Một mô hình luôn dự đoán \"không fraud\" đạt accuracy **{_pct(s['majority_accuracy'])}** nhưng "
            f"Recall = 0 và không phát hiện được giao dịch fraud nào. Vì vậy accuracy không thể là metric duy nhất; "
            f"dự án dùng Precision, Recall, F1, PR-AUC và False Positive Rate. Baseline của PR-AUC "
            f"(mô hình ngẫu nhiên) bằng đúng fraud rate = {_pct(s['fraud_rate'])}.")


def comment_amount(s):
    if s is None:
        return PENDING
    a, leg, f = s["amount_all"], s["amount_legit"], s["amount_fraud"]
    skew_txt = ("lệch phải mạnh (mean > median), cần log-transform hoặc chia bin khi mô hình hoá"
                if a["mean"] > a["median"] and a["skew"] > 1 else "không lệch phải rõ rệt")
    txt = (f"Toàn tập: mean = {a['mean']:,.2f}, median = {a['median']:,.2f}, p95 = {a['p95']:,.2f}, "
           f"p99 = {a['p99']:,.2f}, max = {a['max']:,.2f}, skewness = {a['skew']:.2f} → phân bố {skew_txt}. ")
    if f:
        cmp_ = "thấp hơn" if f["median"] < leg["median"] else "cao hơn"
        txt += (f"Median của fraud = {f['median']:,.2f} so với {leg['median']:,.2f} của non-fraud "
                f"(fraud {cmp_}); mean fraud = {f['mean']:,.2f} vs {leg['mean']:,.2f}; max fraud = {f['max']:,.2f}. ")
    txt += (f"Có **{s['zero_amount']:,}** giao dịch Amount = 0 (trong đó {s['zero_amount_fraud']:,} fraud) → "
            f"cần quy tắc data-quality riêng cho amount bằng 0.")
    return txt


def comment_amount_bins(s):
    if s is None:
        return PENDING
    base = s["fraud_rate"]
    ok = [r for r in s["amount_bins"] if r["n"] >= 100 and r["n_fraud"] >= 3]
    if not ok:
        return "Không nhóm nào đủ mẫu fraud (≥3) để kết luận về xu hướng theo amount."
    hi = max(ok, key=lambda r: r["rate"])
    lo = min(ok, key=lambda r: r["rate"])
    return (f"Nhóm có fraud rate cao nhất (đủ mẫu) là **{hi['key']}**: {_pct(hi['rate'])} "
            f"({hi['n_fraud']}/{hi['n']:,}), gấp {hi['rate'] / base:.1f} lần baseline {_pct(base)}. "
            f"Nhóm thấp nhất là **{lo['key']}**: {_pct(lo['rate'])} ({lo['n_fraud']}/{lo['n']:,}), "
            f"bằng {lo['rate'] / base:.1f} lần baseline. Nhóm có ít fraud (<3) bị loại khỏi so sánh vì nhiễu. "
            f"Ý nghĩa nghiệp vụ: amount có mang thông tin nhưng không đủ riêng lẻ → nên so sánh amount với lịch sử "
            f"của chính thẻ (`amount_vs_avg_7d`) thay vì chỉ dùng ngưỡng tuyệt đối.")


def comment_hour(s):
    if s is None:
        return PENDING
    ok = [r for r in s["hour_table"] if r["n_fraud"] >= 3]
    if not ok:
        return "Không giờ nào đủ mẫu fraud (≥3) để kết luận."
    top = sorted(ok, key=lambda r: r["rate"], reverse=True)[:3]
    items = "; ".join(f"giờ {r['key']}: {_pct(r['rate'])} ({r['n_fraud']}/{r['n']:,})" for r in top)
    return (f"3 giờ tương đối có fraud rate cao nhất (≥3 fraud): {items}; baseline {_pct(s['fraud_rate'])}. "
            f"Lưu ý: `Time` là số giây kể từ giao dịch đầu tiên nên đây là giờ **tương đối**, chưa phải giờ đồng hồ; "
            f"dữ liệu chỉ trải {s['time_span_hours']:.1f} giờ nên mỗi giờ chỉ lặp 1–2 lần → kết luận mang tính gợi ý. "
            f"Trong hệ thống thật nên dùng `hour_of_day` theo múi giờ của chủ thẻ.")


def comment_corr(s):
    if s is None:
        return PENDING
    names = ", ".join(f"{r['feature']} ({r['corr']:+.2f})" for r in s["top_corr_train"][:5])
    return (f"Top 5 theo |r| trên tập train: {names}. Các cột V1–V28 là thành phần PCA ẩn danh nên không diễn giải "
            f"được theo nghiệp vụ; chỉ dùng để dựng **baseline model** trên ULB, không đưa vào feature v1 của "
            f"hệ thống streaming. Tương quan được tính riêng trên train để không rò rỉ thông tin từ validation/test.")


def comment_splits(s):
    if s is None:
        return PENDING
    sp = s["splits"]
    warn = [k for k, v in sp.items() if v["n_fraud"] < 30]
    txt = ("Chia theo thứ tự `Time` (60/20/20), không xáo trộn: " +
           "; ".join(f"{k}: {v['n']:,} dòng, {v['n_fraud']} fraud ({_pct(v['rate'])})" for k, v in sp.items()) + ". ")
    if warn:
        txt += (f"**Cảnh báo:** tập {', '.join(warn)} có dưới 30 giao dịch fraud nên Recall/PR-AUC trên tập đó "
                f"có độ bất định lớn; nên báo cáo kèm khoảng tin cậy (bootstrap) hoặc dùng cross-validation theo thời gian.")
    return txt


def comment_quality(s):
    if s is None:
        return PENDING
    miss = ("không có missing value" if s["missing_total"] == 0
            else f"có {s['missing_total']:,} ô thiếu: {s['missing_by_col']}")
    return (f"Dữ liệu {miss}. Có **{s['dup_rows']:,}** dòng trùng lặp hoàn toàn "
            f"({s['dup_rows'] / s['n_rows']:.3%} tổng số; {s['dup_rows_fraud']:,} dòng thuộc lớp fraud). "
            f"ULB không có `transaction_id` nên không thể phân biệt duplicate do lỗi phát lại với hai giao dịch "
            f"thật giống hệt nhau; trong hệ thống streaming, duplicate được xác định bằng `transaction_id`.")


# --------------------------------------------------------------------------- #
# Render báo cáo
# --------------------------------------------------------------------------- #


def _fmt_table(rows: list[dict], cols: list[tuple[str, callable]]) -> str:
    head = ("| " + " | ".join(c[0] for c in cols) + " |\n|" +
            "|".join("---:" if i else "---" for i, _ in enumerate(cols)) + "|\n")
    return head + "\n".join("| " + " | ".join(fn(r) for _, fn in cols) + " |" for r in rows)


def _rate(r):
    return "n/a" if r["rate"] is None else _pct(r["rate"])


def build_report(stats: dict | None, ieee: dict | None, meta: dict | None) -> str:
    S = stats
    pending = S is None
    banner = ("> ⚠️ **Báo cáo chưa được sinh từ dữ liệu thật.** Đặt `creditcard.csv` vào `data/raw/` rồi chạy "
              "`python scripts/eda_ulb.py` để điền toàn bộ số liệu, biểu đồ và nhận xét.\n\n") if pending else ""

    def v(fn):
        return PENDING if pending else fn(S)

    amount_tbl = PENDING if pending else _fmt_table(S["amount_bins"], [
        ("Nhóm Amount", lambda r: r["key"]), ("Số GD", lambda r: f"{r['n']:,}"),
        ("Số fraud", lambda r: f"{r['n_fraud']:,}"), ("Fraud rate", _rate)])
    hour_tbl = PENDING if pending else _fmt_table(S["hour_table"], [
        ("Giờ tương đối", lambda r: r["key"]), ("Số GD", lambda r: f"{r['n']:,}"),
        ("Số fraud", lambda r: f"{r['n_fraud']:,}"), ("Fraud rate", _rate)])
    split_tbl = PENDING if pending else _fmt_table(
        [{"name": k, **val} for k, val in S["splits"].items()], [
            ("Tập", lambda r: r["name"]), ("Số dòng", lambda r: f"{r['n']:,}"),
            ("Số fraud", lambda r: f"{r['n_fraud']:,}"), ("Fraud rate", _rate),
            ("Time (s) từ–đến", lambda r: f"{r['time_start']:,.0f} – {r['time_end']:,.0f}")])

    def _amt_row(lab, k):
        fr = S["amount_fraud"][k] if S["amount_fraud"] else float("nan")
        return f"| {lab} | {S['amount_all'][k]:,.2f} | {S['amount_legit'][k]:,.2f} | {fr:,.2f} |"

    amt_stat_tbl = PENDING if pending else (
        "| Thống kê | Toàn tập | Non-fraud | Fraud |\n|---|---:|---:|---:|\n" +
        "\n".join(_amt_row(k, k) for k in
                  ["mean", "std", "min", "p25", "median", "p75", "p95", "p99", "max", "skew"]))

    if pending:
        overview = PENDING
        repro_meta = PENDING
    else:
        overview = (f"| Thuộc tính | Giá trị |\n|---|---|\n| Số dòng | {S['n_rows']:,} |\n| Số cột | {S['n_cols']} |\n"
                    f"| Khoảng `Time` | {S['time_min']:,.0f} – {S['time_max']:,.0f} giây "
                    f"(≈ {S['time_span_hours']:.1f} giờ) |\n"
                    f"| Số giao dịch fraud | {S['n_fraud']:,} ({_pct(S['fraud_rate'])}) |")
        m = meta or {}
        repro_meta = (f"- File dữ liệu: `{m.get('file', '?')}` ({m.get('size_bytes', 0):,} bytes)\n"
                      f"- SHA-256: `{m.get('sha256', '?')}`\n"
                      f"- Python {m.get('python', '?')}, pandas {m.get('pandas', '?')}, "
                      f"numpy {m.get('numpy', '?')}, matplotlib {m.get('matplotlib', '?')}\n"
                      f"- Cùng SHA-256 + cùng phiên bản thư viện ⇒ cùng số liệu (script không dùng ngẫu nhiên).")

    if not ieee:
        ieee_block = ("_Chưa cung cấp `train_transaction.csv` (tham số `--ieee-transaction`). Mục 7 vẫn mô tả đầy đủ "
                      "mapping cột; số liệu IEEE-CIS sẽ được điền khi chạy với dữ liệu thật._")
    else:
        a = ieee["amount"]
        ieee_block = (
            f"| Thuộc tính | Giá trị |\n|---|---|\n| Số dòng / cột | {ieee['n_rows']:,} / {ieee['n_cols']} |\n"
            f"| Fraud | {ieee['n_fraud']:,} ({_pct(ieee['fraud_rate'])}) |\n"
            f"| `TransactionID` trùng | {ieee['dup_transaction_id']:,} |\n"
            f"| Khoảng `TransactionDT` | ≈ {ieee['dt_span_days']:.1f} ngày |\n"
            f"| TransactionAmt median / mean / p99 / max | {a['median']:,.2f} / {a['mean']:,.2f} / "
            f"{a['p99']:,.2f} / {a['max']:,.2f} |\n"
            f"| Số cột thiếu > 50% | {ieee['cols_over_50pct_missing']} |\n\n"
            "Fraud rate theo `ProductCD`:\n\n" + _fmt_table(ieee["product_cd"], [
                ("ProductCD", lambda r: r["key"]), ("Số GD", lambda r: f"{r['n']:,}"),
                ("Fraud", lambda r: f"{r['n_fraud']:,}"), ("Fraud rate", _rate)]) +
            "\n\n15 cột thiếu nhiều nhất:\n\n" + _fmt_table(ieee["missing_top15"], [
                ("Cột", lambda r: f"`{r['col']}`"), ("% missing", lambda r: f"{r['pct']:.1f}%")]))

    return f"""# EDA Report - FraudStream (Tuần 1)

> Owner: Phạm Trung Hiếu · Deliverable W1-03 · Dataset chính: ULB Credit Card Fraud; mapping/mở rộng: IEEE-CIS
> Báo cáo này được **sinh tự động** bởi `scripts/eda_ulb.py`; mọi số liệu truy vết được qua `docs/week_1/eda/eda_stats.json`.

{banner}## 1. Mục tiêu và câu hỏi EDA

| # | Câu hỏi | Mục trả lời |
|---|---|---|
| Q1 | Dữ liệu có bao nhiêu giao dịch, tỷ lệ fraud là bao nhiêu? | 2, 4 |
| Q2 | Chất lượng dữ liệu: missing, duplicate, giá trị bất thường? | 3 |
| Q3 | Phân bố `Amount` ra sao, fraud khác non-fraud thế nào? | 5, 6 |
| Q4 | Fraud rate thay đổi theo nhóm amount và theo thời gian ra sao? | 6 |
| Q5 | Cột IEEE-CIS nào map được sang schema giao dịch của dự án? | 7 |
| Q6 | Feature nào đáng đưa vào feature v1 và cần tránh leakage thế nào? | 8, `feature_v1.md` |
| Q7 | Chia train/validation/test theo thời gian có đủ mẫu fraud không? | 6.5 |

## 2. Tổng quan dataset

### 2.1 ULB Credit Card Fraud Detection (dataset chính của EDA)

{overview}

Cột: `Time` (giây kể từ giao dịch đầu tiên), `V1–V28` (thành phần PCA ẩn danh), `Amount`, `Class` (1 = fraud).
Nguồn: https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud

### 2.2 IEEE-CIS Fraud Detection (mapping và mở rộng thiết kế)

{ieee_block}

## 3. Chất lượng dữ liệu (missing, duplicate)

{v(comment_quality)}

## 4. Class imbalance

{v(comment_class)}

## 5. Phân tích `Amount`

{amt_stat_tbl}

{v(comment_amount)}

## 6. Biểu đồ và nhận xét

### 6.1 Phân bố fraud / non-fraud

![Hình 1](figures/fig1_class_distribution.png)

**Nhận xét:** {v(comment_class)}

### 6.2 Phân bố giá trị giao dịch

![Hình 2](figures/fig2_amount_distribution.png)

**Nhận xét:** {v(comment_amount)}

### 6.3 Fraud rate theo nhóm giá trị giao dịch

![Hình 3](figures/fig3_fraud_rate_by_amount.png)

{amount_tbl}

**Nhận xét:** {v(comment_amount_bins)}

### 6.4 Fraud rate theo thời gian (giờ tương đối)

![Hình 4](figures/fig4_fraud_rate_by_time.png)

**Nhận xét:** {v(comment_hour)}

<details><summary>Bảng số liệu theo giờ</summary>

{hour_tbl}

</details>

### 6.5 Bổ sung: boxplot amount, tương quan và chia tập theo thời gian

![Hình 5](figures/fig5_amount_boxplot.png)

![Hình 6](figures/fig6_top_corr.png)

**Nhận xét:** {v(comment_corr)}

**Chia train/validation/test theo thời gian**

{split_tbl}

{v(comment_splits)}

## 7. Mapping IEEE-CIS → schema giao dịch của FraudStream

Schema đích: `transaction_id`, `event_time`, `customer_id`, `card_id_hash`, `merchant_id`, `amount`, `currency`,
`channel`, `device_id_hash`, `ip_hash`, `country`, `transaction_status` (xem `data_dictionary.md`).

| Trường FraudStream | Cột IEEE-CIS | Mức phù hợp | Ghi chú |
|---|---|---|---|
| `transaction_id` | `TransactionID` | Trực tiếp | Thêm tiền tố `tx_`; dùng để deduplicate |
| `event_time` | `TransactionDT` | Một phần | Là số giây tương đối (delta), không phải timestamp; cần chọn mốc gốc giả định (ví dụ 2017-12-01, quy ước phổ biến trong cộng đồng Kaggle, **không** do ban tổ chức xác nhận) |
| `amount` | `TransactionAmt` | Trực tiếp | Đơn vị tiền tệ không nêu rõ; giả định USD |
| `currency` | — | Không có | Gán hằng số khi chuyển đổi |
| `card_id_hash` | `card1`–`card6` | Xấp xỉ | Không có ID thẻ thật; kết hợp `card1`..`card6` (+`addr1`) rồi băm SHA-256 làm proxy |
| `customer_id` | (suy ra từ nhóm thẻ/địa chỉ) | Xấp xỉ | Không có ID khách hàng; dùng proxy từ `card*` + `addr1` |
| `merchant_id` | `ProductCD` | Yếu | Chỉ là mã sản phẩm (5 giá trị), không phải merchant; chỉ dùng tạm cho phân tích |
| `channel` | `ProductCD`, `DeviceType` | Yếu | `DeviceType` (desktop/mobile) chỉ có ở `train_identity` và thiếu nhiều |
| `device_id_hash` | `DeviceInfo`, `id_30`, `id_31`, `id_33` | Xấp xỉ | Từ `train_identity`; ghép các cột rồi băm; nhiều giao dịch không có identity |
| `ip_hash` | — | Không có | Chỉ simulator tạo được |
| `country` | `addr2` (và `addr1`) | Xấp xỉ | `addr2` là mã quốc gia ẩn danh dạng số, không phải ISO |
| `transaction_status` | — | Không có | Dataset chỉ chứa giao dịch đã ghi nhận; không có giao dịch failed |
| nhãn fraud | `isFraud` | Trực tiếp | Chỉ dùng làm label/`fraud_feedback`, **không** đưa vào event stream |

**Các nhóm cột IEEE-CIS khác:** `C1–C14` (đếm ẩn danh), `D1–D15` (khoảng thời gian ẩn danh), `M1–M9` (cờ khớp
thông tin), `V1–V339` (feature do Vesta thiết kế), `dist1/dist2` (khoảng cách), `P_emaildomain/R_emaildomain`.
Các cột này ẩn nghĩa nên không đưa vào feature v1; có thể dùng cho mô hình batch mở rộng.

**Hệ quả:** IEEE-CIS không có `transaction_status` và `ip`, nên tín hiệu *card testing* (nhiều giao dịch thất bại)
và *device sharing theo IP* **không thể kiểm chứng bằng dữ liệu thật**; hai tín hiệu này dựa vào fraud simulator.

## 8. Feature candidates (rút ra từ EDA)

| Feature candidate | Cơ sở từ EDA | Dataset kiểm chứng được |
|---|---|---|
| `amount_vs_avg_7d` | Amount lệch phải; fraud rate khác nhau theo nhóm amount (6.3) | Simulator, IEEE-CIS (proxy thẻ) |
| `log_amount` | Phân bố lệch phải (6.2) | ULB, IEEE-CIS |
| `tx_count_5m`, `amount_sum_1h` | Tín hiệu velocity | Simulator, IEEE-CIS (proxy thẻ) |
| `failed_tx_count_10m` | Card testing | Chỉ simulator |
| `unique_cards_per_device_1h` | Device sharing | Simulator, IEEE-CIS (proxy thiết bị) |
| `is_new_merchant`, `is_new_country` | Hành vi lạ so với lịch sử | Simulator |
| `hour_of_day` | Fraud rate theo giờ (6.4); cần giờ địa phương thật | Simulator, IEEE-CIS (sau khi chọn mốc gốc) |

Đặc tả đầy đủ, công thức và kiểm tra leakage: xem `docs/week_1/eda/feature_v1.md`.

## 9. Giới hạn dữ liệu

1. ULB chỉ có `Time`, `Amount`, `V1–V28`, `Class`: **không có** thẻ, thiết bị, merchant, quốc gia, trạng thái giao dịch → không tính được feature hành vi theo thẻ/thiết bị trên ULB.
2. `Time` là giây tương đối và chỉ trải khoảng 2 ngày → không kiểm tra được cửa sổ 7 ngày, không có tính mùa vụ theo tuần.
3. `V1–V28` ẩn danh (PCA) → không diễn giải được theo nghiệp vụ; chỉ dùng cho baseline.
4. Số lượng fraud rất ít (xem mục 4) → các nhóm/giờ/tập con có thể chỉ có vài fraud, kết luận thống kê có độ bất định lớn.
5. Không có `transaction_id` → không phân biệt được duplicate do phát lại với giao dịch trùng thật.
6. IEEE-CIS: `TransactionDT` tương đối, nhiều cột ẩn nghĩa, missing cao ở nhóm identity/`V*`/`D*`; không có ID thẻ/khách hàng thật.
7. Cả hai dataset đều là dữ liệu quá khứ với nhãn đã được xác nhận; trên hệ thống thật nhãn đến trễ (chargeback), nên đánh giá offline có thể lạc quan hơn thực tế.

## 10. Cách chạy lại

```bash
pip install -r requirements.txt
# đặt creditcard.csv vào data/raw/ (xem data/README.md)
python scripts/eda_ulb.py --data data/raw/creditcard.csv
# tuỳ chọn: thêm profile IEEE-CIS
python scripts/eda_ulb.py --data data/raw/creditcard.csv --ieee-transaction data/raw/ieee/train_transaction.csv
pytest tests/test_eda_pipeline.py
```

Thông tin tái lập:

{repro_meta}
"""


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def run(data: Path, ieee_path: Path | None, docs_dir: Path) -> dict:
    df = load_ulb(data)
    stats = compute_stats(df)
    ieee = profile_ieee_transaction(ieee_path) if ieee_path else None
    figs = make_all_figures(df, stats, docs_dir / "figures")
    meta = {
        "file": Path(data).name, "size_bytes": Path(data).stat().st_size, "sha256": sha256_file(Path(data)),
        "python": platform.python_version(), "pandas": pd.__version__, "numpy": np.__version__,
        "matplotlib": matplotlib.__version__,
    }
    docs_dir.mkdir(parents=True, exist_ok=True)
    (docs_dir / "eda_stats.json").write_text(
        json.dumps({"meta": meta, "ulb": stats, "ieee_cis": ieee}, ensure_ascii=False, indent=2), encoding="utf-8")
    (docs_dir / "eda_report.md").write_text(build_report(stats, ieee, meta), encoding="utf-8")
    print(f"[OK] {stats['n_rows']:,} dòng, {stats['n_fraud']:,} fraud ({stats['fraud_rate']:.4%})")
    print(f"[OK] {len(figs)} biểu đồ -> {docs_dir / 'figures'}")
    print(f"[OK] báo cáo -> {docs_dir / 'eda_report.md'}")
    return stats


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", type=Path, default=ROOT / "data/raw/creditcard.csv")
    ap.add_argument("--ieee-transaction", type=Path, default=None)
    ap.add_argument("--docs-dir", type=Path, default=ROOT / "docs/week_1/eda")
    a = ap.parse_args(argv)
    if not a.data.exists():
        print(f"[LỖI] Không thấy {a.data}. Xem data/README.md để tải ULB creditcard.csv.", file=sys.stderr)
        return 2
    run(a.data, a.ieee_transaction, a.docs_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
