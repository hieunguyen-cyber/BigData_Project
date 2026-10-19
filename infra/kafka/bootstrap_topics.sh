#!/usr/bin/env bash
# Create the four Week 2 topics. Safe to rerun; fail if existing settings differ.
set -euo pipefail

NAMESPACE="${KAFKA_NAMESPACE:-fraudstream}"
POD="${KAFKA_POD:-kafka-0}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVER:-kafka.${NAMESPACE}.svc.cluster.local:9092}"

kubectl wait -n "${NAMESPACE}" --for=condition=Ready "pod/${POD}" --timeout=120s

create_topic() {
    local topic="$1" partitions="$2" retention_ms="$3" description
    echo "Creating/checking ${topic}..."
    kubectl exec -n "${NAMESPACE}" "${POD}" -- \
        /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "${BOOTSTRAP}" \
        --create --if-not-exists --topic "${topic}" \
        --partitions "${partitions}" --replication-factor 1 \
        --config "retention.ms=${retention_ms}" \
        --config cleanup.policy=delete

    description=$(kubectl exec -n "${NAMESPACE}" "${POD}" -- \
        /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "${BOOTSTRAP}" --describe --topic "${topic}")
    echo "${description}"
    if ! echo "${description}" | grep -Eq "PartitionCount:[[:space:]]*${partitions}([[:space:]]|$)"; then
        echo "FAIL: ${topic} must have ${partitions} partition(s)." >&2
        return 1
    fi
    if ! echo "${description}" | grep -Eq 'ReplicationFactor:[[:space:]]*1([[:space:]]|$)'; then
        echo "FAIL: ${topic} must have replication factor 1." >&2
        return 1
    fi
    if ! echo "${description}" | grep -Eq "retention[.]ms=${retention_ms}([,[:space:]]|$)"; then
        echo "FAIL: ${topic} must have retention.ms=${retention_ms}." >&2
        return 1
    fi
    echo "PASS: ${topic} settings match."
}

# Topic                         Partitions   Retention in milliseconds
create_topic transactions_raw        3       604800000   # 7 days
create_topic transactions_invalid    1       259200000   # 3 days
create_topic fraud_alerts            3       604800000   # 7 days
create_topic fraud_feedback          1       2592000000  # 30 days

echo "PASS: all 4 Kafka topics are ready."
