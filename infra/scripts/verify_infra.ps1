<#
.SYNOPSIS
    End-to-End Infrastructure Verification Script for Windows PowerShell
    Deliverable: W2-05 (verify_infra.ps1)
    Contributors: Nguyễn Minh Hoàng, Lê Trọng Đạt, Lê Xuân Nhật Khôi
#>

param (
    [switch]$KafkaOnly,
    [string]$PythonCommand = "python"
)

$Namespace = "fraudstream"
$FailedChecks = 0
$KafkaVerifier = Join-Path $PSScriptRoot "../kafka/smoke_test.py"

if ($KafkaOnly) {
    & $PythonCommand $KafkaVerifier --namespace $Namespace
    if (-not $? -or $LASTEXITCODE -ne 0) { exit 1 }
    exit 0
}

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
    Log-Fail "CoreDNS pods are not in Running state."
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
            Log-Fail "Keyspace 'fraudstream' is not initialized. Run the Cassandra smoke test."
        }

        $queryCheck = kubectl exec -n $Namespace cassandra-0 -- cqlsh -e "SELECT count(*) FROM fraudstream.transaction_risk;" 2>&1
        if ($queryCheck -match "count") {
            Log-Pass "CQL query execution passed without ALLOW FILTERING."
        } else {
            Log-Fail "CQL query verification failed. Run the Cassandra smoke test."
        }
    } else {
        Log-Fail "Cassandra pod is in state: $cassStatus"
    }
} else {
    Log-Fail "Cassandra pod cassandra-0 not found in namespace $Namespace!"
}

# 3. Apache Kafka Verification
Write-Host "`n--- [3/4] Apache Kafka Event Stream ---" -ForegroundColor Cyan
& $PythonCommand $KafkaVerifier --namespace $Namespace
if ($? -and $LASTEXITCODE -eq 0) {
    Log-Pass "Kafka pod/PVC/topics and 100-event producer-consumer test passed."
} else {
    Log-Fail "Kafka verification failed (pod, PVC, topics, or producer-consumer test)."
}

# 4. Apache HDFS Verification
Write-Host "`n--- [4/4] Apache HDFS Data Lake ---" -ForegroundColor Cyan
$nnPods = kubectl get pods -n $Namespace -l app=hdfs-namenode -o jsonpath='{.items[0].metadata.name}' 2>&1
if ($nnPods -and $nnPods -notmatch "Error") {
    $nnStatus = kubectl get pod $nnPods -n $Namespace -o jsonpath='{.status.phase}' 2>&1
    if ($nnStatus -eq "Running") {
        Log-Pass "HDFS NameNode pod $nnPods is Running."
        $requiredPaths = @("/fraud-data/raw", "/fraud-data/cleaned", "/fraud-data/curated", "/fraud-data/late_events", "/fraud-data/checkpoints", "/fraud-data/models")
        foreach ($path in $requiredPaths) {
            kubectl exec -n $Namespace $nnPods -- hdfs dfs -test -d $path 2>&1 | Out-Null
            if ($LASTEXITCODE -eq 0) {
                Log-Pass "HDFS path exists: $path"
            } else {
                Log-Fail "Required HDFS path is missing: $path"
            }
        }
    } else {
        Log-Fail "HDFS NameNode pod is in state $nnStatus"
    }
} else {
    Log-Fail "HDFS NameNode not deployed; full W2 verification cannot pass."
}

$dnPod = kubectl get pods -n $Namespace -l app.kubernetes.io/component=datanode -o jsonpath='{.items[0].metadata.name}' 2>&1
if ($dnPod -and $LASTEXITCODE -eq 0 -and $dnPod -notmatch "Error") {
    $dnStatus = kubectl get pod $dnPod -n $Namespace -o jsonpath='{.status.phase}' 2>&1
    if ($dnStatus -eq "Running") {
        Log-Pass "HDFS DataNode pod $dnPod is Running."
    } else {
        Log-Fail "HDFS DataNode pod is in state $dnStatus"
    }
} else {
    Log-Fail "HDFS DataNode not deployed; HDFS cannot store replicated blocks."
}

foreach ($pvc in @("hdfs-namenode-data-hdfs-namenode-0", "hdfs-datanode-data-hdfs-datanode-0")) {
    $pvcPhase = kubectl get pvc $pvc -n $Namespace -o jsonpath='{.status.phase}' 2>&1
    if ($LASTEXITCODE -eq 0 -and $pvcPhase -eq "Bound") {
        Log-Pass "HDFS PVC $pvc is Bound."
    } else {
        Log-Fail "HDFS PVC $pvc is $pvcPhase; expected Bound."
    }
}

if ($nnPods -and $dnPod -and $nnStatus -eq "Running" -and $dnStatus -eq "Running") {
    $hdfsVerifyPath = "/fraud-data/raw/verify_infra_ps1.json"
    $hdfsVerifyValue = '{"transaction_id":"tx_verify_infra_ps1","event_time":"2026-10-25T00:00:00Z","customer_id":"cus_verify_infra","card_id_hash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","merchant_id":"merchant_verify","amount":1.0,"currency":"VND","channel":"web","device_id_hash":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","ip_hash":"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","country":"VN","transaction_status":"success"}'
    $hdfsVerifyValue | kubectl exec -i -n $Namespace $nnPods -- /bin/bash -c "cat > /tmp/verify_infra_ps1.json" 2>&1 | Out-Null
    kubectl exec -n $Namespace $nnPods -- hdfs dfs -put -f /tmp/verify_infra_ps1.json $hdfsVerifyPath 2>&1 | Out-Null
    $hdfsReadback = kubectl exec -n $Namespace $nnPods -- hdfs dfs -cat $hdfsVerifyPath 2>&1
    if ($LASTEXITCODE -eq 0 -and $hdfsReadback.Trim() -eq $hdfsVerifyValue) {
        Log-Pass "HDFS PowerShell I/O read/write verification passed."
    } else {
        Log-Fail "HDFS PowerShell I/O read/write verification failed."
    }
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
