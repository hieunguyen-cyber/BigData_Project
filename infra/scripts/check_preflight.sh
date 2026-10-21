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
if kubectl get storageclass standard >/dev/null 2>&1; then
    echo "OK (StorageClass 'standard' is available)"
else
    echo "FAILED! (StorageClass 'standard' is required by the W2 StatefulSets)"
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

cat <<EOF | kubectl apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata:
  name: preflight-volume-consumer
  namespace: fraudstream
spec:
  restartPolicy: Never
  containers:
    - name: consumer
      image: busybox:1.36
      command: ["sh", "-c", "echo fraudstream > /data/ready && sleep 20"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: preflight-test-pvc
EOF

if kubectl wait -n fraudstream --for=condition=Ready pod/preflight-volume-consumer --timeout=90s >/dev/null 2>&1 && \
   [[ "$(kubectl get pvc preflight-test-pvc -n fraudstream -o jsonpath='{.status.phase}')" == "Bound" ]] && \
   kubectl exec -n fraudstream preflight-volume-consumer -- test -f /data/ready; then
    echo "OK (PVC is Bound and can be mounted/read-written)"
else
    echo "FAILED! (PVC provisioning or mount test failed)"
    kubectl describe pvc preflight-test-pvc -n fraudstream || true
    kubectl describe pod preflight-volume-consumer -n fraudstream || true
    kubectl delete pod preflight-volume-consumer -n fraudstream --ignore-not-found >/dev/null 2>&1 || true
    kubectl delete pvc preflight-test-pvc -n fraudstream --ignore-not-found >/dev/null 2>&1 || true
    exit 1
fi

kubectl delete pod preflight-volume-consumer -n fraudstream --ignore-not-found >/dev/null 2>&1 || true
kubectl delete pvc preflight-test-pvc -n fraudstream --ignore-not-found >/dev/null 2>&1 || true

echo "============================================================"
echo " Preflight Check PASSED! Cluster is ready for workloads."
echo "============================================================"
