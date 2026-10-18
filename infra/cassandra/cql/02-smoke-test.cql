-- ========================================================================
-- Cassandra Smoke Test Queries for FraudStream
-- Deliverable: W2-04 (Cassandra Smoke Test)
-- Validates:
--  1. Insertion of transactions following data_dictionary.md schema
--  2. Querying by (card_id_hash, event_time) WITHOUT ALLOW FILTERING
--  3. Correct return of transaction_id, risk_score, decision, event_time
-- ========================================================================

USE fraudstream;

-- 1. Insert sample records into transaction_risk
INSERT INTO transaction_risk (
    card_id_hash,
    event_time,
    transaction_id,
    customer_id,
    merchant_id,
    amount,
    currency,
    channel,
    device_id_hash,
    ip_hash,
    country,
    transaction_status,
    risk_score,
    decision,
    trigger_reasons,
    model_version,
    created_at
) VALUES (
    'sha256:7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069',
    '2026-10-18T14:30:00Z',
    'tx_smoke_20261018_0001',
    'cus_test_00128',
    'merchant_0042',
    1250000.0,
    'VND',
    'web',
    'sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
    'sha256:ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb',
    'VN',
    'success',
    0.15,
    'APPROVE',
    ['normal_pattern'],
    'v1.0.0-baseline',
    toTimestamp(now())
);

INSERT INTO transaction_risk (
    card_id_hash,
    event_time,
    transaction_id,
    customer_id,
    merchant_id,
    amount,
    currency,
    channel,
    device_id_hash,
    ip_hash,
    country,
    transaction_status,
    risk_score,
    decision,
    trigger_reasons,
    model_version,
    created_at
) VALUES (
    'sha256:7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069',
    '2026-10-18T14:35:00Z',
    'tx_smoke_20261018_0002',
    'cus_test_00128',
    'merchant_0042',
    45000000.0,
    'VND',
    'web',
    'sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
    'sha256:ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb',
    'VN',
    'success',
    0.89,
    'BLOCK',
    ['high_value_anomaly: amount_vs_avg_7d >= 5', 'velocity_fraud: tx_count_5m >= 5'],
    'v1.0.0-baseline',
    toTimestamp(now())
);

-- 2. Insert alert into fraud_alert
INSERT INTO fraud_alert (
    card_id_hash,
    event_time,
    transaction_id,
    customer_id,
    merchant_id,
    amount,
    currency,
    risk_score,
    decision,
    alert_status,
    trigger_reasons,
    assigned_analyst,
    created_at
) VALUES (
    'sha256:7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069',
    '2026-10-18T14:35:00Z',
    'tx_smoke_20261018_0002',
    'cus_test_00128',
    'merchant_0042',
    45000000.0,
    'VND',
    0.89,
    'BLOCK',
    'PENDING',
    ['high_value_anomaly', 'velocity_fraud'],
    'unassigned',
    toTimestamp(now())
);

-- 3. Query 1: Single partition query with time range filter
-- Notice: NO "ALLOW FILTERING" is needed because card_id_hash is partition key
-- and event_time is clustering key!
SELECT
    card_id_hash,
    event_time,
    transaction_id,
    amount,
    currency,
    risk_score,
    decision,
    trigger_reasons
FROM fraudstream.transaction_risk
WHERE card_id_hash = 'sha256:7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069'
  AND event_time >= '2026-10-18T14:00:00Z';

-- 4. Query 2: Retrieve alerts for operations
SELECT
    card_id_hash,
    event_time,
    transaction_id,
    risk_score,
    decision,
    alert_status
FROM fraudstream.fraud_alert
WHERE card_id_hash = 'sha256:7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069'
  AND event_time >= '2026-10-18T14:00:00Z';
