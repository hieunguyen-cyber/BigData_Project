<#
.SYNOPSIS
    Submit and monitor FraudStream Batch Jobs on Windows PowerShell
    Deliverable: W4-05
    Owner: Lê Xuân Nhật Khôi
#>

param (
    [string]$BatchId = "batch_fixture_v1",
    [string]$Fixture = "tests\fixtures\batch\v1\transactions.jsonl",
    [string]$ReferenceTime = "2026-10-04T10:00:00Z",
    [int]$WatermarkMinutes = 10,
    [string]$PythonCommand = "python"
)

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$SubmitScript = Join-Path $ScriptDir "submit_batch.py"

& $PythonCommand $SubmitScript --batch-id $BatchId --fixture $Fixture --reference-time $ReferenceTime --watermark-minutes $WatermarkMinutes
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
