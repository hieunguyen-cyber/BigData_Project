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
$sc = kubectl get storageclass -o jsonpath='{.items[*].metadata.name}' 2>&1
if ($sc -match "standard") {
    Write-Host "OK (Default StorageClass 'standard' found)" -ForegroundColor Green
} elseif ($sc.Length -gt 0) {
    Write-Host "OK (StorageClasses found: $sc)" -ForegroundColor Green
} else {
    Write-Host "FAILED! (No StorageClass found)" -ForegroundColor Red
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
Start-Sleep -Seconds 3
$pvcPhase = kubectl get pvc preflight-test-pvc -n fraudstream -o jsonpath='{.status.phase}' 2>&1
kubectl delete pvc preflight-test-pvc -n fraudstream 2>&1 | Out-Null

if ($pvcPhase -match "Bound" -or $pvcPhase -match "Pending") {
    Write-Host "OK (PVC provisioner responds correctly: $pvcPhase)" -ForegroundColor Green
} else {
    Write-Host "WARNING (PVC phase: $pvcPhase)" -ForegroundColor Yellow
}

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host " Preflight Check PASSED! Cluster is ready for workloads." -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Cyan
