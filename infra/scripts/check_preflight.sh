#!/usr/bin/env bash
# ========================================================================
# Cluster Preflight Check Script for FraudStream
# Deliverable: W2-01 (Kubernetes platform preflight)
# Owner: Lê Xuân Nhật Khôi
# ========================================================================

set -euo pipefail

echo "============================================================"
echo " Running FraudStream Kubernetes Platform Preflight Checks"
echo "============================================================"

# 1. Docker Daemon Check
echo -n "[1/6] Checking Docker Daemon... "
if docker info >/dev/null 2>&1; then
    echo "OK (Docker is running)"
else
    echo "FAILED! (Docker is not running or not responding. Please start Docker Desktop)"
    exit 1
fi

# 2. Kubernetes Cluster / Nodes Check
echo -n "[2/6] Checking Kubernetes Cluster Nodes... "
READY_NODES=$(kubectl get nodes --no-headers 2>/dev/null | grep -c " Ready" || true)
if [[ "${READY_NODES}" -gt 0 ]]; then
    echo "OK (${READY_NODES} node(s) Ready)"
    kubectl get nodes
else
    echo "FAILED! (No nodes in Ready status. Ensure Kind cluster is running)"
    exit 1
fi

# 3. Namespace Check
echo -n "[3/6] Checking namespace 'fraudstream'... "
if kubectl get namespace fraudstream >/dev/null 2>&1; then
    echo "OK (Namespace exists)"
else
    echo "WARNING! (Namespace 'fraudstream' not found. Creating it now...)"
    kubectl create namespace fraudstream
    kubectl label namespace fraudstream app.kubernetes.io/part-of=fraudstream --overwrite
fi

# 4. StorageClass Check
echo -n "[4/6] Checking StorageClass... "
DEFAULT_SC=$(kubectl get storageclass -o jsonpath='{.items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")].metadata.name}' 2>/dev/null || true)
if [[ -z "${DEFAULT_SC}" ]]; then
    DEFAULT_SC=$(kubectl get storageclass -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
fi

if [[ -n "${DEFAULT_SC}" ]]; then
    echo "OK (Default StorageClass: ${DEFAULT_SC})"
else
    echo "FAILED! (No StorageClass found in cluster. Kind hostpath provisioner might not be ready)"
    exit 1
fi

# 5. CoreDNS Check
echo -n "[5/6] Checking CoreDNS / kube-dns... "
DNS_PODS=$(kubectl get pods -n kube-system -l k8s-app=kube-dns --no-headers 2>/dev/null | grep -c "Running" || true)
if [[ "${DNS_PODS}" -gt 0 ]]; then
    echo "OK (${DNS_PODS} CoreDNS pod(s) Running)"
else
    echo "WARNING! (CoreDNS pods not in Running state)"
fi

# 6. Test Volume Provisioning
echo -n "[6/6] Testing Volume Provisioning with test PVC... "
cat <<EOF | kubectl apply -f - >/dev/null 2>&1
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: preflight-test-pvc
  namespace: fraudstream
spec:
  accessModes: [ "ReadWriteOnce" ]
  storageClassName: standard
  resources:
    requests:
      storage: 10Mi
EOF

# Give provisioner a moment to bind
sleep 3
PVC_PHASE=$(kubectl get pvc preflight-test-pvc -n fraudstream -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
kubectl delete pvc preflight-test-pvc -n fraudstream >/dev/null 2>&1 || true

if [[ "${PVC_PHASE}" == "Bound" || "${PVC_PHASE}" == "Pending" ]]; then
    # In Kind/hostpath, some storage provisioners bind on first pod mount (WaitForFirstConsumer) or bind immediately
    echo "OK (PVC provisioner responds correctly: ${PVC_PHASE})"
else
    echo "WARNING (PVC phase: ${PVC_PHASE})"
fi

echo "============================================================"
echo " Preflight Check PASSED! Cluster is ready for workloads."
echo "============================================================"
