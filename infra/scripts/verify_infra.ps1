<#
.SYNOPSIS
    End-to-End Infrastructure Verification Script for Windows PowerShell
    Deliverable: W2-05 (verify_infra.ps1)
    Contributors: Nguyễn Minh Hoàng, Lê Trọng Đạt, Lê Xuân Nhật Khôi
#>

$Namespace = "fraudstream"
$FailedChecks = 0

Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host " FraudStream Infrastructure Verification (verify_infra.ps1)" -ForegroundColor Cyan
Write-Host " Namespace: $Namespace" -ForegroundColor Cyan
Write-Host " Time: $(Get-Date -Format 'yyyy-MM-ddTHH:mm:ssZ')" -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan

function Log-Pass ($msg) {
    Write-Host " [PASS] $msg" -ForegroundColor Green
}

function Log-Fail ($msg) {
    Write-Host " [FAIL] $msg" -ForegroundColor Red
    $script:FailedChecks++
}

function Log-Warn ($msg) {
    Write-Host " [WARN] $msg" -ForegroundColor Yellow
}

# 1. Platform Verification
Write-Host "`n--- [1/4] Platform & Kubernetes Cluster ---" -ForegroundColor Cyan
$nodes = kubectl get nodes --no-headers 2>&1
if ($nodes -match " Ready") {
    Log-Pass "Kubernetes cluster nodes are in Ready state."
} else {
    Log-Fail "No Kubernetes nodes in Ready state!"
}

$ns = kubectl get namespace $Namespace 2>&1
if ($LASTEXITCODE -eq 0) {
    Log-Pass "Namespace '$Namespace' exists."
} else {
    Log-Fail "Namespace '$Namespace' not found!"
}

$dnsPods = kubectl get pods -n kube-system -l k8s-app=kube-dns --no-headers 2>&1
if ($dnsPods -match "Running") {
    Log-Pass "CoreDNS service is Running."
} else {
    Log-Warn "CoreDNS pods not in Running state."
}

$pvcs = kubectl get pvc -n $Namespace --no-headers 2>&1
$pendingPvcs = ($pvcs | Select-String -Pattern "Pending").Count
if ($pendingPvcs -gt 0) {
    Log-Fail "Found Pending PVCs in $Namespace"
} else {
    $boundPvcs = ($pvcs | Select-String -Pattern "Bound").Count
    Log-Pass "PVC status healthy ($boundPvcs Bound PVCs)."
}

# 2. Apache Cassandra Verification
Write-Host "`n--- [2/4] Apache Cassandra Serving Store ---" -ForegroundColor Cyan
$cassPod = kubectl get pod cassandra-0 -n $Namespace 2>&1
if ($LASTEXITCODE -eq 0) {
    $cassStatus = kubectl get pod cassandra-0 -n $Namespace -o jsonpath='{.status.phase}' 2>&1
    if ($cassStatus -eq "Running") {
        Log-Pass "Cassandra pod cassandra-0 is Running."

        $nodetool = kubectl exec -n $Namespace cassandra-0 -- nodetool status 2>&1
        if ($nodetool -match "^UN\s+") {
            Log-Pass "Cassandra node state is UN (Up Normal)."
        } else {
            Log-Fail "Cassandra node is not in UN state!"
        }

        $ksCheck = kubectl exec -n $Namespace cassandra-0 -- cqlsh -e "DESCRIBE KEYSPACE fraudstream;" 2>&1
        if ($ksCheck -match "fraudstream") {
            Log-Pass "Keyspace 'fraudstream' exists and is accessible."
        } else {
            Log-Warn "Keyspace 'fraudstream' not initialized. Run smoke test or bootstrap."
        }

        $queryCheck = kubectl exec -n $Namespace cassandra-0 -- cqlsh -e "SELECT count(*) FROM fraudstream.transaction_risk;" 2>&1
        if ($queryCheck -match "count") {
            Log-Pass "CQL query execution passed without ALLOW FILTERING."
        } else {
            Log-Warn "CQL query verification skipped or table empty."
        }
    } else {
        Log-Fail "Cassandra pod is in state: $cassStatus"
    }
} else {
    Log-Fail "Cassandra pod cassandra-0 not found in namespace $Namespace!"
}

# 3. Apache Kafka Verification
Write-Host "`n--- [3/4] Apache Kafka Event Stream ---" -ForegroundColor Cyan
$kafkaPods = kubectl get pods -n $Namespace -l app.kubernetes.io/name=kafka -o jsonpath='{.items[0].metadata.name}' 2>&1
if ($kafkaPods -and $kafkaPods -notmatch "Error") {
    $kStatus = kubectl get pod $kafkaPods -n $Namespace -o jsonpath='{.status.phase}' 2>&1
    if ($kStatus -eq "Running") {
        Log-Pass "Kafka pod $kafkaPods is Running."
    } else {
        Log-Fail "Kafka pod $kafkaPods is in state $kStatus"
    }
} else {
    Log-Warn "Kafka pod not yet deployed (Waiting for Hoàng's deliverable: infra/kafka/)"
}

# 4. Apache HDFS Verification
Write-Host "`n--- [4/4] Apache HDFS Data Lake ---" -ForegroundColor Cyan
$nnPods = kubectl get pods -n $Namespace -l app=hdfs-namenode -o jsonpath='{.items[0].metadata.name}' 2>&1
if ($nnPods -and $nnPods -notmatch "Error") {
    $nnStatus = kubectl get pod $nnPods -n $Namespace -o jsonpath='{.status.phase}' 2>&1
    if ($nnStatus -eq "Running") {
        Log-Pass "HDFS NameNode pod $nnPods is Running."
    } else {
        Log-Fail "HDFS NameNode pod is in state $nnStatus"
    }
} else {
    Log-Warn "HDFS NameNode not yet deployed (Waiting for Đạt's deliverable: infra/hdfs/)"
}

# Summary
Write-Host "`n==================================================================" -ForegroundColor Cyan
Write-Host " Infrastructure Verification Summary" -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan
if ($FailedChecks -eq 0) {
    Write-Host " STATUS: ALL VERIFIED CHECKS PASSED" -ForegroundColor Green
    exit 0
} else {
    Write-Host " STATUS: FAILED ($FailedChecks checks failed)" -ForegroundColor Red
    exit 1
}
