-- ========================================================================
-- Keyspace and Schema Bootstrap for FraudStream Serving Store
-- Project: FraudStream - Real-Time Online Payment Fraud Detection
-- Deliverable: W2-04 (Cassandra schema & bootstrap)
-- Owner: Lê Xuân Nhật Khôi
-- ========================================================================

-- 1. Keyspace Definition (Idempotent)
-- Local development uses SimpleStrategy with replication_factor 1.
-- In multi-node production, switch to NetworkTopologyStrategy: {'datacenter1': 3}
CREATE KEYSPACE IF NOT EXISTS fraudstream
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
} AND durable_writes = true;

USE fraudstream;

-- 2. Table: transaction_risk
-- Access Pattern: Retrieve real-time risk scores and decisions by card_id_hash within a time range.
-- Partition Key: card_id_hash (locates card behavior history on the same partition)
-- Clustering Key: event_time DESC, transaction_id ASC
-- Note: Queries filtering by card_id_hash and event_time DO NOT require ALLOW FILTERING.
CREATE TABLE IF NOT EXISTS transaction_risk (
    card_id_hash text,
    event_time timestamp,
    transaction_id text,
    customer_id text,
    merchant_id text,
    amount decimal,
    currency text,
    channel text,
    device_id_hash text,
    ip_hash text,
    country text,
    transaction_status text,
    risk_score double,
    decision text,               -- 'APPROVE', 'REVIEW', 'BLOCK'
    trigger_reasons list<text>,  -- E.g. ['velocity_fraud: tx_count_5m >= 5', 'failed_tx_count_10m >= 3']
    model_version text,
    created_at timestamp,
    PRIMARY KEY ((card_id_hash), event_time, transaction_id)
) WITH CLUSTERING ORDER BY (event_time DESC, transaction_id ASC)
  AND comment = 'Stores online risk assessment results per card partitioned by card_id_hash';

-- 3. Table: fraud_alert
-- Access Pattern: Fetch unresolved or historical fraud alerts by card_id_hash and event_time.
-- Partition Key: card_id_hash
-- Clustering Key: event_time DESC, transaction_id ASC
CREATE TABLE IF NOT EXISTS fraud_alert (
    card_id_hash text,
    event_time timestamp,
    transaction_id text,
    customer_id text,
    merchant_id text,
    amount decimal,
    currency text,
    risk_score double,
    decision text,               -- 'REVIEW' or 'BLOCK'
    alert_status text,           -- 'PENDING', 'INVESTIGATED', 'FALSE_POSITIVE', 'CONFIRMED_FRAUD'
    trigger_reasons list<text>,
    assigned_analyst text,
    created_at timestamp,
    PRIMARY KEY ((card_id_hash), event_time, transaction_id)
) WITH CLUSTERING ORDER BY (event_time DESC, transaction_id ASC)
  AND comment = 'Stores fraud alerts for operations dashboard and analyst investigation';
