# Batch Pipeline v1 — FraudStream

> Owner: Phạm Trung Hiếu · Week 3 deliverable · Design contract for implementation in the next batch phase.
> Status: **Design/fixture specified; Spark batch ETL and HDFS runtime are not claimed as implemented by this document.**

## 1. Purpose and scope

Define a deterministic path from immutable JSONL transaction events to validated, deduplicated, date-partitioned Parquet. This document turns `docs/general/data_dictionary.md` and `docs/week_1/eda/feature_v1.md` into an implementation contract. It does not claim that Spark or HDFS jobs already run.

In scope: raw retention, schema/constraint checks, deduplication by `transaction_id`, invalid/duplicate/late routing, Parquet schema, UTC date partitioning, batch metrics, reproducibility, and leakage checks. Model training and online scoring are out of scope.

## 2. Contract sources and invariants

- Event schema: exactly the 12 required v1 fields documented in `docs/general/data_dictionary.md`.
- `transaction_id` is the idempotency/deduplication key; do not deduplicate on amount/time/card alone.
- `event_time` is the business/event timestamp. Normalize offset-aware timestamps to UTC for processing; canonical output is UTC. The published contract examples use `Z`. A strict parser should reject timezone-naive timestamps; producers should emit canonical `Z` timestamps.
- `amount` must be numeric and greater than zero. Extremely large amounts remain valid under DQ-07; do not silently drop them. The threshold and `extreme_amount` flag implementation must be explicitly configured before production use because the current contract does not specify a numeric threshold.
- Classification order: `invalid -> duplicate -> late -> valid`; one mutually exclusive classification per input event.
- Watermark: 10 minutes, matching the data dictionary and Week 3 handoff target.
- No real public dataset, credentials, raw payment identifiers, or PII belongs in the fixture/repository.

## 3. HDFS lifecycle and paths

Use the existing top-level HDFS layout defined by Week 3. Nested directories below these roots are proposed batch conventions, not a claim that the paths have already been created in a running cluster.

| Layer | Path convention | Format | Lifecycle / rule |
|---|---|---|---|
| Immutable raw | `/fraud-data/raw/transactions_raw/ingest_date=YYYY-MM-DD/batch_id=<id>/` | Original JSONL, UTF-8 | Preserve input bytes; never overwrite a prior batch. Record source, ingestion timestamp, batch ID, count, and checksum in a manifest. |
| Invalid quarantine | `/fraud-data/raw/invalid/ingest_date=YYYY-MM-DD/batch_id=<id>/` | JSONL + reason codes | Quarantine malformed JSON or records violating DQ-01..DQ-06/DQ-10. This uses a subdirectory under the existing `/raw` root; coordinate with the streaming handoff owner before integration. |
| Cleaned | `/fraud-data/cleaned/event_date=YYYY-MM-DD/` | Parquet, Snappy | Schema-valid, unique, non-late records after canonicalization. Preserve transaction fields and add `event_date`. |
| Curated | `/fraud-data/curated/event_date=YYYY-MM-DD/` | Parquet, Snappy | Analytics/training-ready rows after documented feature enrichment and temporal checks. Week 3 does not yet implement features here. |
| Late events | `/fraud-data/late_events/ingest_date=YYYY-MM-DD/batch_id=<id>/` | JSONL initially; Parquet after controlled backfill if agreed | Keep late events separate from normal online output. Backfill must be idempotent and record the correction/reprocessing batch. |
| Checkpoints | `/fraud-data/checkpoints/<job_name>/` | Spark checkpoint metadata | Runtime state only; do not treat as business output or delete during normal reruns. |
| Models | `/fraud-data/models/<model_name>/version=<version>/` | Model artifact + metadata | Immutable, versioned artifacts; include training-data window, feature version, code/config version, and evaluation metrics. |

**Path integration note:** the agreed HDFS foundation lists six roots (`raw`, `cleaned`, `curated`, `late_events`, `checkpoints`, `models`). There is no separate top-level `invalid` root in the current target, so v1 proposes `/fraud-data/raw/invalid/`. The team must align this with `docs/week_3/batch_streaming_handoff.md` before wiring sinks. Do not create a seventh top-level path silently.

## 4. Pipeline stages

### Stage A — Register and preserve input

1. Assign a stable `batch_id` and UTC `ingest_date`.
2. Persist the original JSONL file under the raw convention before transformations.
3. Write a manifest containing source/topic, file list, byte size, SHA-256, line count, ingestion time, schema version, and job version.
4. Treat a rerun of the same `batch_id` and checksum as idempotent; reject a conflicting checksum for an already committed batch ID.

### Stage B — Parse and validate

1. Read JSONL as one JSON object per non-empty line. A JSON parse failure is an invalid/quarantined record with `DQ-PARSE` and a source file/line locator; it cannot be passed into the current Python validator as a dict.
2. Check all 12 required fields, types, allowed values, UTC/timezone semantics, currency/country formats, and synthetic hash patterns using DQ-01..DQ-06 and DQ-10.
3. Supply an explicit processing-time reference to DQ-05 so a batch rerun is reproducible. Do not let the result depend on an implicit wall clock. The reference timestamp belongs in the batch manifest/config.
4. Keep DQ-07 extreme amounts valid. Flagging threshold remains a configurable follow-up decision.

### Stage C — Deduplicate

1. Deduplicate only records that passed validation, using `transaction_id`.
2. For exact repeated transaction IDs, retain one deterministic winner. In a production source, choose the earliest committed source position/ingestion sequence; if the same ID has conflicting payloads, quarantine the conflict for investigation rather than arbitrarily choosing by event time.
3. Count duplicates even though duplicate rows are not written to cleaned/curated.
4. Deduplication must run before dataset splitting and before downstream feature generation.

### Stage D — Handle late events

The DQ-09 definition is relative to the maximum event time observed so far and the 10-minute watermark. A correct streaming classification uses arrival/source order and a running `max_event_time_seen`; a batch job must **not** sort the entire batch by `event_time` and compare each row with the future maximum, because that leaks future knowledge into the classification.

Preferred integration: consume the durable `late_events` sink from streaming and preserve the original routing decision. If the batch job must classify an arrival-ordered JSONL fixture itself, process the fixture in source order with a running maximum, as the v1 fixture test does. If the source format does not preserve arrival order or a prior classification, late status is not reconstructible exactly; record that limitation rather than guessing.

Late records are stored separately under `/fraud-data/late_events/`. Controlled backfill may later recompute affected dates, but it must be idempotent, versioned, and must not overwrite the historical raw file. Record both the original and correction batch IDs.

### Stage E — Write cleaned and curated Parquet

- Cleaned v1 contains only structurally valid, non-duplicate, non-late transaction rows, plus `event_date` derived from `event_time` normalized to UTC.
- Curated v1 initially mirrors cleaned schema until a feature-enrichment job is explicitly implemented. Do not claim feature columns exist simply because they are described in `feature_v1.md`.
- Compression: Snappy.
- Partition: `event_date=YYYY-MM-DD`, computed as `to_date(event_time_utc)`. This is the initial partition contract; `event_hour` is not required for v1.
- Write to a temporary/run-specific location, validate row counts/schema/partitions, then commit/rename the completed output where supported. Do not expose partially written output as a successful batch.
- Avoid tiny files: the implementation phase should coalesce/repartition based on volume and target file size; do not force one file per event or one partition directory per transaction.

## 5. Output schemas

### 5.1 Cleaned Parquet v1

| Column | Type | Nullability | Rule |
|---|---|---|---|
| `transaction_id` | string | non-null | Unique within committed output scope |
| `event_time` | timestamp (UTC semantics) | non-null | Parsed from contract timestamp; normalize to UTC |
| `customer_id` | string | non-null | Synthetic identifier |
| `card_id_hash` | string | non-null | `sha256:` + 64 lowercase hex |
| `merchant_id` | string | non-null | Synthetic identifier |
| `amount` | double/decimal | non-null | `> 0`; choose decimal precision in Spark implementation and document it |
| `currency` | string | non-null | Three uppercase letters |
| `channel` | string | non-null | `web`, `mobile`, `api` |
| `device_id_hash` | string | non-null | Synthetic SHA-256 |
| `ip_hash` | string | non-null | Synthetic SHA-256 |
| `country` | string | non-null | Two uppercase letters |
| `transaction_status` | string | non-null | `success` or `failed` |
| `event_date` | date | non-null | UTC date derived from `event_time`; partition column |

`event_date` is a storage/partition column derived deterministically from the event; it is not an additional source field. If the Spark writer stores the partition column only in directory names, the reader must reconstruct it consistently.

### 5.2 Curated Parquet v1

At Week 3, curated schema equals cleaned schema. Future feature columns must be added under a versioned feature contract and include definitions, type/nullability, event-time cutoff, cold-start behavior, and anti-leakage tests. Do not include `Class`, `isFraud`, `fraud_feedback`, chargeback outcomes, or future review decisions as inference-time features.

## 6. Acceptance metrics and definitions

Metrics are per input batch and must be recorded in a machine-readable run manifest. Classification counts are mutually exclusive and sum to the number of parsed transaction records; malformed JSON lines are tracked separately as `parse_invalid` and included in `invalid_total`.

| Metric | Definition | Acceptance rule |
|---|---|---|
| `input_records` | Number of non-empty input lines presented to parser, including malformed JSON | Equals line count in raw manifest |
| `parsed_records` | Lines successfully parsed into JSON objects | `parsed_records + parse_invalid = input_records` |
| `parse_invalid` | Lines that fail JSON parsing | Count matches quarantine output |
| `valid_records` | Schema-valid, unique, non-late records written to cleaned | Equals cleaned row count |
| `invalid_records` | Schema/constraint-invalid events plus parse failures | Equals invalid quarantine count; include reason-code breakdown |
| `duplicate_records` | Additional occurrences of already-seen `transaction_id` among valid events | Counted, excluded from cleaned and curated |
| `late_records` | Structurally valid unique records routed to late sink | Equals late output count; excluded from normal cleaned/curated output |
| `cleaned_records` | Rows committed to cleaned Parquet | Must equal `valid_records` |
| `curated_records` | Rows committed to curated Parquet for v1 | Must equal cleaned count until enrichment is implemented |
| `distinct_transaction_ids` | Distinct IDs in cleaned output | Equals `cleaned_records` |
| `output_schema_match` | Required output columns/types/nullability match v1 | Must be true |
| `partition_date_match` | For every row, `event_date == UTC_DATE(event_time)` and folder partition agrees | Must be true for all rows |
| `source_checksum_match` | Raw file SHA-256 matches the pre-transform manifest | Must be true; raw is immutable |
| `anti_leakage_checks_passed` | Feature temporal/label leakage assertions executed by the feature job | Must be true before a feature-bearing curated dataset is approved; Week 3 fixture checks schema-level invariants only |

**Fixture expected counts:** 4 input records → 1 valid, 1 invalid, 1 duplicate, 1 late. Therefore cleaned candidate rows = 1 and curated v1 rows = 1. This is a contract test, not a model-quality result.

## 7. Anti-leakage and temporal integrity

1. Use event time for feature windows, never processing time; processing time is only used for operational freshness/DQ-05 checks with an explicit reference.
2. At time `t`, no feature may use an event with `event_time > t`.
3. Historical features such as `amount_vs_avg_7d`, `is_new_merchant`, and `is_new_country` exclude the current transaction and all future transactions.
4. For each target event, a truncate test compares features computed from the full data with features computed after removing all rows after `t`; results must match.
5. Deduplicate before train/validation/test splitting. Split chronologically (starting point 60/20/20); fit imputation/scaling/encoding and tune thresholds on train/validation only, never test.
6. Do not include fraud labels or delayed `fraud_feedback` in scoring features. They may be targets for training only after label availability is modeled.
7. A temporal gap equal to the longest feature window (7 days for the current v1 history feature) is recommended when dataset duration permits; explicitly document when a dataset is too short.
8. Late-event backfill may change historical features; record the backfill version and do not silently mix pre- and post-correction feature snapshots.

## 8. Versioning, idempotency, and observability

- Contract version: `transaction_event_v1`; fixture version: `1.0.0`; feature contract: `feature_v1`.
- A batch manifest should include `batch_id`, `run_id`, source paths, raw checksum, schema version, code commit, config version, explicit reference processing time, watermark, counts, output paths, output schema fingerprint, and status.
- Output commits must be idempotent for a repeated run with the same source checksum and config. A changed source/config creates a new run/version, not an untracked overwrite.
- Log per-rule invalid counts and sample-safe record locators; avoid printing full sensitive payloads.
- `checkpoints` are Spark execution state, not the durable audit manifest. Keep audit manifests separately with each batch output or in a future agreed metadata location.

## 9. Reproduce the v1 fixture

From repository root:

```bash
python -m pytest tests/test_batch_fixture.py -q
python -m pytest tests -q
```

Fixture files:

- `tests/fixtures/batch/v1/transactions.jsonl`
- `tests/fixtures/batch/v1/manifest.json`
- `tests/fixtures/batch/v1/expected_results.json`

The test uses a fixed reference processing time and preserves source line order. It tests the current Python contract classifier and output acceptance expectations; it does **not** test Spark, Parquet serialization, or a live HDFS deployment.

## 10. Week 3 acceptance checklist

- [x] Input contract traces to `docs/general/data_dictionary.md` (12 fields and DQ rules).
- [x] Raw/cleaned/curated/late/checkpoint/model paths and lifecycle defined.
- [x] Invalid routing defined as a proposed subpath under the existing raw root; cross-owner confirmation flagged.
- [x] Deduplication key and deterministic conflict behavior specified.
- [x] UTC `event_date` partition and Snappy Parquet specified.
- [x] Batch metrics and exact fixture counts defined.
- [x] Future leakage, label leakage, train-only transforms, and late-event caveats specified.
- [x] Versioned synthetic fixture and automated contract test included.
- [ ] Spark job and live HDFS writes: **future implementation; not a Week 3 completed runtime claim**.
- [ ] Confirm invalid HDFS subpath and raw ingestion conventions with the streaming/HDFS owners.
