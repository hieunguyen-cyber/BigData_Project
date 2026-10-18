<#
.SYNOPSIS
    Cassandra Smoke Test & Persistence Verification Script for Windows PowerShell
    Deliverable: W2-04 (Cassandra Smoke Test)
    Owner: Lê Xuân Nhật Khôi
#>

param (
    [switch]$TestPersistence
)

$ErrorActionPreference = "Stop"
$Namespace = "fraudstream"
$PodName = "cassandra-0"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$CqlDir = Join-Path $ScriptDir "cql"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host " Starting Cassandra Smoke Test for FraudStream" -ForegroundColor Cyan
Write-Host " Target Namespace: $Namespace" -ForegroundColor Cyan
Write-Host " Pod: $PodName" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

# 1. Check Pod status
Write-Host "[1/5] Checking Cassandra pod status..." -ForegroundColor Yellow
$podCheck = kubectl get pod $PodName -n $Namespace 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Error "Pod $PodName not found in namespace $Namespace!"
    exit 1
}

Write-Host "Waiting for pod $PodName to be Ready (timeout 180s)..." -ForegroundColor Yellow
kubectl wait --namespace $Namespace --for=condition=ready pod/$PodName --timeout=180s
if ($LASTEXITCODE -ne 0) {
    Write-Error "Pod $PodName did not reach Ready state!"
    exit 1
}

# 2. Check Node health
Write-Host "[2/5] Checking Cassandra cluster health (nodetool status)..." -ForegroundColor Yellow
$nodetoolOut = kubectl exec -n $Namespace $PodName -- nodetool status
$isUN = $nodetoolOut | Select-String -Pattern "^UN\s+"
if (-not $isUN) {
    Write-Error "Cassandra node is not in 'UN' (Up Normal) state!"
    Write-Host $nodetoolOut
    exit 1
}
Write-Host "Cassandra node is healthy: Up Normal (UN)" -ForegroundColor Green
Write-Host $nodetoolOut

# 3. Bootstrap CQL
Write-Host "[3/5] Bootstrapping keyspace and tables (01-bootstrap.cql)..." -ForegroundColor Yellow
$initCql = Join-Path $CqlDir "01-bootstrap.cql"
Get-Content -Raw $initCql | kubectl exec -i -n $Namespace $PodName -- cqlsh
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to execute 01-bootstrap.cql!"
    exit 1
}
Write-Host "Bootstrap completed successfully." -ForegroundColor Green

# 4. Smoke Test Queries
Write-Host "[4/5] Executing smoke test inserts and selects (02-smoke-test.cql)..." -ForegroundColor Yellow
$smokeCql = Join-Path $CqlDir "02-smoke-test.cql"
$cqlOutput = Get-Content -Raw $smokeCql | kubectl exec -i -n $Namespace $PodName -- cqlsh
Write-Host $cqlOutput

if ($cqlOutput -match "tx_smoke_20261018_0001" -and $cqlOutput -match "BLOCK") {
    Write-Host ">>> Smoke Test Query Check: PASS (Found expected transactions and decisions without ALLOW FILTERING)" -ForegroundColor Green
} else {
    Write-Error ">>> Smoke Test Query Check: FAIL (Missing expected records in CQL output)"
    exit 1
}

# 5. Check Persistence
if ($TestPersistence) {
    Write-Host "[5/5] Testing Storage Persistence across Pod Restart..." -ForegroundColor Yellow
    Write-Host "Deleting pod $PodName (StatefulSet will auto-recreate)..."
    kubectl delete pod $PodName -n $Namespace --wait=$false

    Start-Sleep -Seconds 5
    Write-Host "Waiting for pod to be recreated and ready..."
    kubectl wait --namespace $Namespace --for=condition=ready pod/$PodName --timeout=180s

    Write-Host "Verifying data persistence after pod restart..."
    $checkQuery = "SELECT transaction_id, risk_score, decision FROM fraudstream.transaction_risk WHERE card_id_hash = 'sha256:7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069' AND event_time >= '2026-10-18T14:00:00Z';"
    $verifyOutput = kubectl exec -n $Namespace $PodName -- cqlsh -e $checkQuery
    Write-Host $verifyOutput

    if ($verifyOutput -match "tx_smoke_20261018_0001") {
        Write-Host ">>> Data Persistence Check: PASS (Data preserved on PVC across pod recreation)" -ForegroundColor Green
    } else {
        Write-Error ">>> Data Persistence Check: FAIL (Data was lost after pod restart)"
        exit 1
    }
} else {
    Write-Host "[5/5] Skipping pod restart test. (Pass -TestPersistence to run)" -ForegroundColor Cyan
}

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host " Cassandra Smoke Test Completed Successfully! (ALL PASS)" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Cyan
