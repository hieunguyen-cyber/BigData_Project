#!/usr/bin/env bash
# ========================================================================
# Cassandra Smoke Test & Persistence Verification Script
# Deliverable: W2-04 (Cassandra Smoke Test)
# Owner: Lê Xuân Nhật Khôi
# ========================================================================

set -euo pipefail

NAMESPACE="fraudstream"
POD_NAME="cassandra-0"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CQL_DIR="${SCRIPT_DIR}/cql"

echo "============================================================"
echo " Starting Cassandra Smoke Test for FraudStream"
echo " Target Namespace: ${NAMESPACE}"
echo " Pod: ${POD_NAME}"
echo "============================================================"

# 1. Check Pod Existence & Ready status
echo "[1/5] Checking Cassandra pod status..."
if ! kubectl get pod "${POD_NAME}" -n "${NAMESPACE}" >/dev/null 2>&1; then
    echo "ERROR: Pod ${POD_NAME} not found in namespace ${NAMESPACE}!"
    exit 1
fi

echo "Waiting for pod ${POD_NAME} to be Ready (timeout 180s)..."
kubectl wait --namespace "${NAMESPACE}" \
  --for=condition=ready pod/"${POD_NAME}" \
  --timeout=180s

# 2. Check Cassandra Node Status via nodetool
echo "[2/5] Checking Cassandra cluster health (nodetool status)..."
NODE_STATUS=$(kubectl exec -n "${NAMESPACE}" "${POD_NAME}" -- nodetool status | grep -E '^UN\s+' || true)
if [[ -z "${NODE_STATUS}" ]]; then
    echo "ERROR: Cassandra node is not in 'UN' (Up Normal) state!"
    kubectl exec -n "${NAMESPACE}" "${POD_NAME}" -- nodetool status
    exit 1
fi
echo "Cassandra node is healthy: Up Normal (UN)"
kubectl exec -n "${NAMESPACE}" "${POD_NAME}" -- nodetool status

# 3. Bootstrap Keyspace & Tables
echo "[3/5] Bootstrapping keyspace and tables (01-bootstrap.cql)..."
kubectl exec -i -n "${NAMESPACE}" "${POD_NAME}" -- cqlsh < "${CQL_DIR}/01-bootstrap.cql"
echo "Bootstrap completed successfully."

# 4. Run Smoke Test Queries (Insert & Select)
echo "[4/5] Executing smoke test inserts and selects (02-smoke-test.cql)..."
OUTPUT=$(kubectl exec -i -n "${NAMESPACE}" "${POD_NAME}" -- cqlsh < "${CQL_DIR}/02-smoke-test.cql")
echo "${OUTPUT}"

# Verify expected values in output
if echo "${OUTPUT}" | grep -q "tx_smoke_20261018_0001" && echo "${OUTPUT}" | grep -q "BLOCK"; then
    echo ">>> Smoke Test Query Check: PASS (Found expected transactions and decisions without ALLOW FILTERING)"
else
    echo ">>> Smoke Test Query Check: FAIL (Missing expected records in CQL output)"
    exit 1
fi

# 5. Check Persistence (Optional / Triggerable via flag)
if [[ "${1:-}" == "--test-persistence" ]]; then
    echo "[5/5] Testing Storage Persistence across Pod Restart..."
    echo "Deleting pod ${POD_NAME} (StatefulSet will auto-recreate)..."
    kubectl delete pod "${POD_NAME}" -n "${NAMESPACE}" --wait=false

    echo "Waiting for pod to be deleted and recreated..."
    sleep 5
    kubectl wait --namespace "${NAMESPACE}" \
      --for=condition=ready pod/"${POD_NAME}" \
      --timeout=180s

    echo "Verifying data persistence after pod restart..."
    RESTART_QUERY_OUTPUT=$(kubectl exec -n "${NAMESPACE}" "${POD_NAME}" -- cqlsh -e "SELECT transaction_id, risk_score, decision FROM fraudstream.transaction_risk WHERE card_id_hash = 'sha256:7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069' AND event_time >= '2026-10-18T14:00:00Z';")
    echo "${RESTART_QUERY_OUTPUT}"

    if echo "${RESTART_QUERY_OUTPUT}" | grep -q "tx_smoke_20261018_0001"; then
        echo ">>> Data Persistence Check: PASS (Data preserved on PVC across pod recreation)"
    else
        echo ">>> Data Persistence Check: FAIL (Data was lost after pod restart)"
        exit 1
    fi
else
    echo "[5/5] Skipping pod restart test. (Pass --test-persistence to run)"
fi

echo "============================================================"
echo " Cassandra Smoke Test Completed Successfully! (ALL PASS)"
echo "============================================================"
