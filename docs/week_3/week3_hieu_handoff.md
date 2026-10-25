# Week 3 technical handoff — Phạm Trung Hiếu

> Scope: batch data-quality foundation plus verified EDA/feature notes for the Checkpoint 1 owner. This is a handoff aid, not a replacement for `docs/checkpoints/checkpoint_1.md`.

## 1. Batch deliverables prepared

- `docs/week_3/batch_pipeline_v1.md`: raw → validation/quarantine → dedup/late routing → cleaned/curated Parquet contract.
- `tests/fixtures/batch/v1/transactions.jsonl`: four synthetic cases in deterministic source order.
- `tests/fixtures/batch/v1/manifest.json`: fixture version, explicit reference processing time, watermark, and case definitions.
- `tests/fixtures/batch/v1/expected_results.json`: expected classifications, counts, output expectation, and leakage assertions.
- `tests/test_batch_fixture.py`: executable regression test against `data-generator/schema.py`.

The fixture is a contract test, not a benchmark. It does not contain the real ULB dataset or personal data. The batch Spark job, Parquet writes, and live HDFS integration remain future/runtime work and must not be described as completed unless run evidence is collected.

## 2. Verified EDA facts to retain in Checkpoint 1

Source: `docs/week_1/eda/eda_report.md` and the repository's stored EDA artifacts. These are documented historical EDA results; because `data/raw/creditcard.csv` is intentionally not committed, they were not independently recomputed during this handoff.

- ULB Credit Card Fraud Detection: 284,807 rows; 492 fraud; fraud rate 0.173%; non-fraud:fraud ratio approximately 578:1.
- 1,081 exact duplicate rows reported; no missing values reported.
- Amount is strongly right-skewed: mean 88.35, median 22.00, p95 365.00, p99 1,017.97, max 25,691.16, skewness 16.98.
- Dataset has 1,825 `Amount = 0` rows (27 fraud). This is an EDA observation and highlights a mismatch with the FraudStream event contract (`amount > 0`); do not change the simulator/data contract silently. Document a dataset-specific cleaning/mapping decision for any baseline experiment.
- The stored chronological split is train 170,884 rows / 360 fraud, validation 56,961 / 57 fraud, test 56,962 / 75 fraud. Do not randomly shuffle this dataset for the primary temporal evaluation.
- Six figures are documented under `docs/week_1/eda/figures/`: class distribution, amount distribution, fraud rate by amount, fraud rate by relative time, amount boxplot, and top correlations.

## 3. Dataset limitations that must be said aloud

- ULB contains `Time`, `Amount`, `V1–V28`, `Class`; it does not contain transaction card/customer, merchant, device, IP, country, or transaction status fields needed for most behavioural features in `feature_v1.md`.
- `Time` is elapsed seconds relative to the first event, not a real timestamp. The observed span is about 48 hours, so a 7-day history feature cannot be validated on ULB.
- `V1–V28` are anonymized PCA components and are not business-interpretable.
- Labels are retrospective; a live system may receive chargeback/review feedback later. Historical offline results may be optimistic compared with real-time scoring.
- The 1,081 duplicate rows are exact row duplicates in the documented EDA, not necessarily proven duplicate payment events. The real dataset lacks the project's `transaction_id`, so they cannot validate production deduplication semantics.
- IEEE-CIS is an optional extension described in the report; its mapping to card/device/customer/country is approximate, and it lacks fields such as `transaction_status` and IP. Mark proxy fields clearly.

## 4. Feature v1 anti-leakage summary

Source: `docs/week_1/eda/feature_v1.md`.

- Features: `tx_count_5m`, `amount_sum_1h`, `failed_tx_count_10m`, `unique_cards_per_device_1h`, `amount_vs_avg_7d`, `is_new_merchant`, `is_new_country`.
- Event-time windows use only information available at the scoring instant. Historical features exclude the current transaction where specified.
- Never use `Class`/`isFraud`/`fraud_feedback` or post-transaction outcomes as scoring features.
- Deduplicate before splitting; split chronologically 60/20/20; fit transforms on train only; tune on validation; report test once.
- A 7-day temporal gap is recommended where the dataset supports it; ULB's ~48-hour span does not support that gap or a valid 7-day history evaluation.

## 5. Open integration decisions (do not silently claim resolved)

1. Invalid HDFS routing: Week 3 names six top-level HDFS roots but no separate invalid root. This handoff proposes `/fraud-data/raw/invalid/`; align with `docs/week_3/batch_streaming_handoff.md` and HDFS owner before implementation.
2. Strict timestamp canonicalization: contract examples specify UTC `Z`, while `parse_event_time` currently accepts any offset-aware ISO timestamp and normalizes it to UTC. Decide whether to reject non-`Z` input at ingestion or accept and canonicalize.
3. DQ-05 reproducibility: batch runs must use an explicit reference processing time, not an implicit wall clock.
4. Extreme amount threshold: DQ-07 says retain and flag, but no threshold is defined yet. Do not invent one in a production claim.
5. Late-event semantics: exact DQ-09 classification depends on event arrival/source order and running max event time; a batch that sees only unordered events cannot reconstruct this reliably without persisted routing metadata.

## 6. Suggested evidence for Checkpoint 1 owner

- Run `python -m pytest tests -q` and record the actual output/time in the checkpoint only after running it on the target checkout.
- Show `docs/week_1/eda/eda_report.md` class-imbalance figure and explain why accuracy is insufficient.
- Show `docs/week_1/eda/feature_v1.md` leakage rules and one truncate-test example when implemented.
- Show `docs/week_3/batch_pipeline_v1.md` and fixture test as **design/contract evidence**, not HDFS/Spark runtime evidence.
- Keep HDFS, Spark batch/streaming, model, and dashboard statuses aligned with actual manifests/logs.
