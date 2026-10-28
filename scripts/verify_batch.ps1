<#
.SYNOPSIS
    Verify FraudStream Batch Job output on Windows PowerShell
    Deliverable: W4-04
    Owner: Lê Xuân Nhật Khôi
#>

param (
    [string]$BatchId = "batch_fixture_v1",
    [string]$Fixture = "tests\fixtures\batch\v1\transactions.jsonl",
    [string]$PythonCommand = "python"
)

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$VerifyScript = Join-Path $ScriptDir "verify_batch.py"

& $PythonCommand $VerifyScript --batch-id $BatchId --fixture $Fixture
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
