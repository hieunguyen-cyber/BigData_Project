<#
.SYNOPSIS
    Kubernetes Platform Preflight Check Script for Windows PowerShell
    Deliverable: W2-01 (Kubernetes platform preflight)
    Owner: Lê Xuân Nhật Khôi
#>

$ErrorActionPreference = "Continue"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host " Running FraudStream Kubernetes Platform Preflight Checks" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

# 1. Docker Daemon Check
Write-Host -NoNewline "[1/6] Checking Docker Daemon... "
$dockerInfo = docker info 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "OK (Docker is running)" -ForegroundColor Green
} else {
    Write-Host "FAILED! (Docker is not running. Please start Docker Desktop)" -ForegroundColor Red
    exit 1
}

# 2. Kubernetes Nodes
Write-Host -NoNewline "[2/6] Checking Kubernetes Cluster Nodes... "
$nodes = kubectl get nodes --no-headers 2>&1
$readyNodes = ($nodes | Select-String -Pattern " Ready").Count
if ($readyNodes -gt 0) {
    Write-Host "OK ($readyNodes node(s) Ready)" -ForegroundColor Green
    kubectl get nodes
} else {
    Write-Host "FAILED! (No nodes Ready. Ensure Kind cluster is running: 'kind create cluster --config infra/kind/kind-cluster.yaml')" -ForegroundColor Red
    exit 1
}

# 3. Namespace Check
Write-Host -NoNewline "[3/6] Checking namespace 'fraudstream'... "
$ns = kubectl get namespace fraudstream 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "OK (Namespace exists)" -ForegroundColor Green
} else {
    Write-Host "WARNING! (Namespace 'fraudstream' not found. Creating it...)" -ForegroundColor Yellow
    kubectl create namespace fraudstream
    kubectl label namespace fraudstream app.kubernetes.io/part-of=fraudstream --overwrite
}

# 4. StorageClass Check
Write-Host -NoNewline "[4/6] Checking StorageClass... "
$sc = kubectl get storageclass standard 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "OK (StorageClass 'standard' is available)" -ForegroundColor Green
} else {
    Write-Host "FAILED! (StorageClass 'standard' is required by the W2 StatefulSets)" -ForegroundColor Red
    exit 1
}

# 5. CoreDNS Check
Write-Host -NoNewline "[5/6] Checking CoreDNS... "
$dnsPods = kubectl get pods -n kube-system -l k8s-app=kube-dns --no-headers 2>&1
$runningDns = ($dnsPods | Select-String -Pattern "Running").Count
if ($runningDns -gt 0) {
    Write-Host "OK ($runningDns CoreDNS pod(s) Running)" -ForegroundColor Green
} else {
    Write-Host "WARNING! (CoreDNS pods not Ready)" -ForegroundColor Yellow
}

# 6. Test Volume Provisioning
Write-Host -NoNewline "[6/6] Testing Volume Provisioning with test PVC... "
$testPvcYaml = @"
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
"@

$testPvcYaml | kubectl apply -f - | Out-Null
$testPodYaml = @"
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
"@
$testPodYaml | kubectl apply -f - | Out-Null
kubectl wait -n fraudstream --for=condition=Ready pod/preflight-volume-consumer --timeout=90s 2>&1 | Out-Null
$ready = $LASTEXITCODE -eq 0
$pvcPhase = kubectl get pvc preflight-test-pvc -n fraudstream -o jsonpath='{.status.phase}' 2>&1
kubectl exec -n fraudstream preflight-volume-consumer -- test -f /data/ready 2>&1 | Out-Null
$mounted = $LASTEXITCODE -eq 0

if ($ready -and $pvcPhase -eq "Bound" -and $mounted) {
    Write-Host "OK (PVC is Bound and can be mounted/read-written)" -ForegroundColor Green
} else {
    Write-Host "FAILED! (PVC provisioning or mount test failed)" -ForegroundColor Red
    kubectl describe pvc preflight-test-pvc -n fraudstream
    kubectl describe pod preflight-volume-consumer -n fraudstream
    kubectl delete pod preflight-volume-consumer -n fraudstream --ignore-not-found 2>&1 | Out-Null
    kubectl delete pvc preflight-test-pvc -n fraudstream --ignore-not-found 2>&1 | Out-Null
    exit 1
}

kubectl delete pod preflight-volume-consumer -n fraudstream --ignore-not-found 2>&1 | Out-Null
kubectl delete pvc preflight-test-pvc -n fraudstream --ignore-not-found 2>&1 | Out-Null

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host " Preflight Check PASSED! Cluster is ready for workloads." -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Cyan
