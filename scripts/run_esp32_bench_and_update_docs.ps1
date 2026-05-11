# SPDX-License-Identifier: MIT
param(
    [Parameter(Mandatory = $true)][string]$Port,
    [int]$Baud = 115200,
    [int]$OpenTimeoutSec = 30,
    [int]$RunTimeoutSec = 240,
    [switch]$RunStress,
    [string]$BenchRoot = "bench/loxdb_esp32_s3_bench_head",
    [string]$OutDir = "docs/results"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ensure-Dir {
    param([Parameter(Mandatory = $true)][string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        New-Item -ItemType Directory -Path $Path | Out-Null
    }
}

Ensure-Dir -Path $OutDir

$sha = (git rev-parse --short HEAD).Trim()
$ts = Get-Date -Format "yyyyMMdd_HHmmss"

$runBench = Join-Path $BenchRoot "run_bench.ps1"
if (-not (Test-Path -LiteralPath $runBench)) {
    throw "Bench runner not found: $runBench"
}

$detLog = Join-Path $OutDir "esp32_deterministic_${ts}_${sha}_$($Port.ToLower()).log"
$balLog = Join-Path $OutDir "esp32_balanced_${ts}_${sha}_$($Port.ToLower()).log"
$stressLog = Join-Path $OutDir "esp32_stress_${ts}_${sha}_$($Port.ToLower()).log"

Write-Host "== Running deterministic on $Port =="
& $runBench `
    -Port $Port `
    -Baud $Baud `
    -OpenTimeoutSec $OpenTimeoutSec `
    -RunTimeoutSec $RunTimeoutSec `
    -CommandScript "resetdb;run_det;metrics" `
    -LogPath $detLog

Write-Host ""
Write-Host "== Running balanced on $Port =="
& $runBench `
    -Port $Port `
    -Baud $Baud `
    -OpenTimeoutSec $OpenTimeoutSec `
    -RunTimeoutSec $RunTimeoutSec `
    -CommandScript "profile balanced;run;metrics" `
    -LogPath $balLog

if ($RunStress) {
    Write-Host ""
    Write-Host "== Running stress on $Port =="
    & $runBench `
        -Port $Port `
        -Baud $Baud `
        -OpenTimeoutSec $OpenTimeoutSec `
        -RunTimeoutSec ([Math]::Max($RunTimeoutSec, 900)) `
        -CommandScript "profile stress;run;metrics" `
        -LogPath $stressLog
}

Write-Host ""
Write-Host "== Updating docs/BENCHMARKS.md =="
if ($RunStress) {
    & "scripts/update_benchmarks_md.ps1" -DeterministicLog $detLog -BalancedLog $balLog -StressLog $stressLog -OutPath "docs/BENCHMARKS.md"
} else {
    & "scripts/update_benchmarks_md.ps1" -DeterministicLog $detLog -BalancedLog $balLog -OutPath "docs/BENCHMARKS.md"
}

Write-Host ""
Write-Host "Logs:"
Write-Host "  $detLog"
Write-Host "  $balLog"
if ($RunStress) {
    Write-Host "  $stressLog"
}
