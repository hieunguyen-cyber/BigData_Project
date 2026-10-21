#!/usr/bin/env bash
# ========================================================================
# End-to-End Infrastructure Verification Script for FraudStream
# Deliverable: W2-05 (verify_infra.sh)
# Contributors: Nguyễn Minh Hoàng, Lê Trọng Đạt, Lê Xuân Nhật Khôi
# ========================================================================

set -euo pipefail

NAMESPACE="fraudstream"
FAILED_CHECKS=0
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

# Hoang can verify Kafka independently before Cassandra/HDFS are deployed.
if [[ "${1:-}" == "--kafka-only" && "$#" -eq 1 ]]; then
    exec "${PYTHON_BIN}" "${SCRIPT_DIR}/../kafka/smoke_test.py" --namespace "${NAMESPACE}"
elif [[ "$#" -ne 0 ]]; then
    echo "Usage: bash infra/scripts/verify_infra.sh [--kafka-only]" >&2
    exit 2
fi

echo "=================================================================="
echo " FraudStream Infrastructure Verification (verify_infra.sh)"
echo " Namespace: ${NAMESPACE}"
echo " Time: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo "=================================================================="

log_pass() {
    echo -e " [\033[32mPASS\033[0m] $1"
}

log_fail() {
    echo -e " [\033[31mFAIL\033[0m] $1"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
}

log_warn() {
    echo -e " [\033[33mWARN\033[0m] $1"
}

# ------------------------------------------------------------------
# 1. Cluster & Platform Verification (Owner: Lê Xuân Nhật Khôi)
# ------------------------------------------------------------------
echo ""
echo "--- [1/4] Platform & Kubernetes Cluster ---"
if kubectl get nodes --no-headers 2>/dev/null | grep -q " Ready"; then
    log_pass "Kubernetes cluster nodes are in Ready state."
else
    log_fail "No Kubernetes nodes in Ready state!"
fi

if kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1; then
    log_pass "Namespace '${NAMESPACE}' exists."
else
    log_fail "Namespace '${NAMESPACE}' not found!"
fi

# Check CoreDNS
if kubectl get pods -n kube-system -l k8s-app=kube-dns --no-headers 2>/dev/null | grep -q "Running"; then
    log_pass "CoreDNS service is Running."
else
    log_fail "CoreDNS pods are not in Running state."
fi

# Check PVCs in fraudstream
echo "Checking PersistentVolumeClaims in ${NAMESPACE}..."
PENDING_PVCS=$(kubectl get pvc -n "${NAMESPACE}" --no-headers 2>/dev/null | grep "Pending" || true)
if [[ -n "${PENDING_PVCS}" ]]; then
    log_fail "Found Pending PVCs in ${NAMESPACE}:"
    kubectl get pvc -n "${NAMESPACE}"
else
    BOUND_COUNT=$(kubectl get pvc -n "${NAMESPACE}" --no-headers 2>/dev/null | grep -c "Bound" || true)
    log_pass "PVC status healthy (${BOUND_COUNT} Bound PVCs)."
fi

# ------------------------------------------------------------------
# 2. Apache Cassandra Verification (Owner: Lê Xuân Nhật Khôi)
# ------------------------------------------------------------------
echo ""
echo "--- [2/4] Apache Cassandra Serving Store ---"
if kubectl get pod cassandra-0 -n "${NAMESPACE}" >/dev/null 2>&1; then
    POD_STATUS=$(kubectl get pod cassandra-0 -n "${NAMESPACE}" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
    if [[ "${POD_STATUS}" == "Running" ]]; then
        log_pass "Cassandra pod cassandra-0 is Running."

        # Check nodetool status
        if kubectl exec -n "${NAMESPACE}" cassandra-0 -- nodetool status 2>/dev/null | grep -E -q '^UN\s+'; then
            log_pass "Cassandra node state is UN (Up Normal)."
        else
            log_fail "Cassandra node is not in UN state!"
        fi

        # Check Keyspace & Schema
        KEYSPACE_CHECK=$(kubectl exec -n "${NAMESPACE}" cassandra-0 -- cqlsh -e "DESCRIBE KEYSPACE fraudstream;" 2>/dev/null || true)
        if echo "${KEYSPACE_CHECK}" | grep -q "fraudstream"; then
            log_pass "Keyspace 'fraudstream' exists and is accessible."
        else
            log_fail "Keyspace 'fraudstream' is not initialized. Run the Cassandra smoke test."
        fi

        # Check Smoke Test Query without ALLOW FILTERING
        QUERY_TEST=$(kubectl exec -n "${NAMESPACE}" cassandra-0 -- cqlsh -e "SELECT count(*) FROM fraudstream.transaction_risk;" 2>/dev/null || true)
        if echo "${QUERY_TEST}" | grep -q "count"; then
            log_pass "CQL query execution passed without ALLOW FILTERING."
        else
            log_fail "CQL query verification failed. Run the Cassandra smoke test."
        fi
    else
        log_fail "Cassandra pod cassandra-0 is in status: ${POD_STATUS}"
    fi
else
    log_fail "Cassandra pod cassandra-0 not found in namespace ${NAMESPACE}!"
fi

# ------------------------------------------------------------------
# 3. Apache Kafka Verification (Owner: Nguyễn Minh Hoàng)
# ------------------------------------------------------------------
echo ""
echo "--- [3/4] Apache Kafka Event Stream ---"
if "${PYTHON_BIN}" "${SCRIPT_DIR}/../kafka/smoke_test.py" --namespace "${NAMESPACE}"; then
    log_pass "Kafka pod/PVC/topics and 100-event producer-consumer test passed."
else
    log_fail "Kafka verification failed (pod, PVC, topics, or producer-consumer test)."
fi

# ------------------------------------------------------------------
# 4. Apache HDFS Verification (Owner: Lê Trọng Đạt)
# ------------------------------------------------------------------
echo ""
echo "--- [4/4] Apache HDFS Data Lake ---"
NAMENODE_POD=$(kubectl get pods -n "${NAMESPACE}" -l app=hdfs-namenode -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || \
               kubectl get pods -n "${NAMESPACE}" -l app.kubernetes.io/component=namenode -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

if [[ -n "${NAMENODE_POD}" ]]; then
    NN_STATUS=$(kubectl get pod "${NAMENODE_POD}" -n "${NAMESPACE}" -o jsonpath='{.status.phase}')
    if [[ "${NN_STATUS}" == "Running" ]]; then
        log_pass "HDFS NameNode pod ${NAMENODE_POD} is Running."
        
        # Verify 6 required data-lake layout folders
        REQUIRED_PATHS=("/fraud-data/raw" "/fraud-data/cleaned" "/fraud-data/curated" "/fraud-data/late_events" "/fraud-data/checkpoints" "/fraud-data/models")
        HDFS_DIRS=$(kubectl exec -n "${NAMESPACE}" "${NAMENODE_POD}" -- hdfs dfs -ls /fraud-data 2>/dev/null || true)
        for p in "${REQUIRED_PATHS[@]}"; do
            if echo "${HDFS_DIRS}" | grep -q "${p}"; then
                log_pass "HDFS path exists: ${p}"
            else
                log_fail "Required HDFS path is missing: ${p}"
            fi
        done
    else
        log_fail "HDFS NameNode pod is in state ${NN_STATUS}"
    fi
else
    log_fail "HDFS NameNode not deployed; full W2 verification cannot pass."
fi

# ------------------------------------------------------------------
# Summary & Exit Code
# ------------------------------------------------------------------
echo ""
echo "=================================================================="
echo " Infrastructure Verification Summary"
echo "=================================================================="
if [[ "${FAILED_CHECKS}" -eq 0 ]]; then
    echo -e " STATUS: \033[32mALL VERIFIED CHECKS PASSED\033[0m"
    echo "=================================================================="
    exit 0
else
    echo -e " STATUS: \033[31mFAILED (${FAILED_CHECKS} checks failed)\033[0m"
    echo "=================================================================="
    exit 1
fi
