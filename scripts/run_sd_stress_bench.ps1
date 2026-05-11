# SPDX-License-Identifier: MIT
param(
    [Parameter(Mandatory = $true)][string]$Port,
    [int]$Baud = 115200,
    [int]$OpenTimeoutSec = 45,
    [int]$DurationSec = 120,
    [string]$Profile = "soak",  # smoke|soak|stress
    [string]$Mode = "all",      # all|kv|ts|rel
    [string]$Verify = "on",     # on|off
    [switch]$ResetDb,
    [switch]$FormatDb,
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

function Read-UntilAnyPattern {
    param(
        [Parameter(Mandatory = $true)][System.IO.Ports.SerialPort]$Serial,
        [Parameter(Mandatory = $true)][string[]]$Patterns,
        [Parameter(Mandatory = $true)][int]$TimeoutSec,
        [ref]$Buffer,
        [ref]$MatchedPattern
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $chunk = $Serial.ReadExisting()
            if (-not [string]::IsNullOrEmpty($chunk)) {
                $Buffer.Value += $chunk
                foreach ($pattern in $Patterns) {
                    if ($Buffer.Value -match [regex]::Escape($pattern)) {
                        $MatchedPattern.Value = $pattern
                        return $true
                    }
                }
            }
        } catch {
        }
        Start-Sleep -Milliseconds 50
    }
    return $false
}

function Write-Cmd {
    param(
        [Parameter(Mandatory = $true)][System.IO.Ports.SerialPort]$Serial,
        [Parameter(Mandatory = $true)][string]$Cmd
    )
    $Serial.WriteLine($Cmd)
}

Ensure-Dir -Path $OutDir

$sha = (git rev-parse --short HEAD).Trim()
$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$portTag = $Port.ToLower()

$rawLogPath = Join-Path $OutDir "esp32_sd_stress_${ts}_${sha}_${portTag}.log"
$csvPath = Join-Path $OutDir "esp32_sd_stress_${ts}_${sha}_${portTag}.csv"
$mdPath = Join-Path $OutDir "esp32_sd_stress_${ts}_${sha}_${portTag}.md"

$serial = New-Object System.IO.Ports.SerialPort $Port, $Baud, ([System.IO.Ports.Parity]::None), 8, ([System.IO.Ports.StopBits]::One)
$serial.NewLine = "`n"
$serial.ReadTimeout = 100
$serial.WriteTimeout = 1000
$serial.DtrEnable = $false
$serial.RtsEnable = $false

$fullLog = ""
$csvRows = New-Object System.Collections.Generic.List[object]
$sdReady = "[OK] loxdb SD stress bench ready"
$fatal = "[FATAL]"
$wrongBenchPrompts = @("loxdb-bench>", "microdb-bench>")
$readyPatterns = @($sdReady, $fatal) + $wrongBenchPrompts

try {
    Write-Host "Opening $Port @ $Baud..."
    $serial.Open()
    Start-Sleep -Milliseconds 800

    # Wake output.
    $serial.WriteLine("")

    $buf = ""
    $matched = ""
    $ready = Read-UntilAnyPattern -Serial $serial -Patterns $readyPatterns -TimeoutSec $OpenTimeoutSec -Buffer ([ref]$buf) -MatchedPattern ([ref]$matched)
    $fullLog += $buf
    if (-not $ready) {
        throw "SD stress bench ready marker not detected on $Port within $OpenTimeoutSec s."
    }
    if ($matched -eq $fatal) {
        throw "Device reported [FATAL] during startup."
    }
    if ($wrongBenchPrompts -contains $matched) {
        throw "Detected terminal bench firmware ($matched). Flash bench/loxdb_esp32_s3_sd_stress_bench/loxdb_esp32_s3_sd_stress_bench.ino to $Port and retry."
    }

    # Deterministic start: pause -> (format/reset) -> config -> run.
    Write-Cmd -Serial $serial -Cmd "pause"
    Start-Sleep -Milliseconds 100

    if ($FormatDb) {
        Write-Host "Formatting DB image..."
        Write-Cmd -Serial $serial -Cmd "formatdb"
        Start-Sleep -Milliseconds 200
    } elseif ($ResetDb) {
        Write-Cmd -Serial $serial -Cmd "resetdb"
        Start-Sleep -Milliseconds 200
    }

    Write-Cmd -Serial $serial -Cmd ("profile {0}" -f $Profile)
    Write-Cmd -Serial $serial -Cmd ("mode {0}" -f $Mode)
    Write-Cmd -Serial $serial -Cmd ("verify {0}" -f $Verify)
    Write-Cmd -Serial $serial -Cmd "stats"
    Write-Cmd -Serial $serial -Cmd "run"

    $start = Get-Date
    $deadline = $start.AddSeconds($DurationSec)
    while ((Get-Date) -lt $deadline) {
        $chunk = ""
        try { $chunk = $serial.ReadExisting() } catch { $chunk = "" }

        if (-not [string]::IsNullOrEmpty($chunk)) {
            $fullLog += $chunk

            foreach ($line in ($chunk -split "`r?`n")) {
                if ($line -match "^\[PRESSURE\]\s+kv=(\d+)\s+ts=(\d+)\s+rel=(\d+)\s+wal=(\d+)\s+risk=(\d+)\s+ops=(\d+)") {
                    $csvRows.Add([pscustomobject]@{
                        ts_iso  = (Get-Date).ToString("o")
                        kv_pct  = [int]$Matches[1]
                        ts_pct  = [int]$Matches[2]
                        rel_pct = [int]$Matches[3]
                        wal_pct = [int]$Matches[4]
                        risk_pct = [int]$Matches[5]
                        ops     = [int]$Matches[6]
                    })
                }
                if ($line -match "^\[FATAL\]") {
                    throw "Device reported [FATAL] during run."
                }
            }
        }

        Start-Sleep -Milliseconds 50
    }

    Write-Cmd -Serial $serial -Cmd "pause"
    Write-Cmd -Serial $serial -Cmd "stats"
    Start-Sleep -Milliseconds 250

    # Drain tail output.
    $tail = ""
    try { $tail = $serial.ReadExisting() } catch { $tail = "" }
    if (-not [string]::IsNullOrEmpty($tail)) { $fullLog += $tail }
}
finally {
    $utf8Bom = New-Object System.Text.UTF8Encoding $true
    [System.IO.File]::WriteAllText($rawLogPath, $fullLog, $utf8Bom)

    if ($csvRows.Count -gt 0) {
        $csvRows | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $csvPath
    }

    $md = @()
    $md += ("# SD stress run - {0}" -f $ts)
    $md += ""
    $md += ("- port: {0}" -f $Port)
    $md += ("- repo commit: {0}" -f $sha)
    $md += ("- profile: {0}" -f $Profile)
    $md += ("- mode: {0}" -f $Mode)
    $md += ("- verify: {0}" -f $Verify)
    $md += ("- duration: {0}s" -f $DurationSec)
    $md += ""
    $md += "Artifacts:"
    $md += ""
    $md += ("- raw log: {0}" -f ([IO.Path]::GetFileName($rawLogPath)))
    if (Test-Path -LiteralPath $csvPath) {
        $md += ("- pressure CSV: {0}" -f ([IO.Path]::GetFileName($csvPath)))
    }
    $md += ""
    $md += "Notes:"
    $md += ""
    $md += "- The bench prints `[PRESSURE]`, `[STATS]`, `[BENCH]` lines periodically; see the raw log for full context."

    [System.IO.File]::WriteAllText($mdPath, ($md -join "`r`n"), $utf8Bom)

    if ($serial.IsOpen) { $serial.Close() }
}

Write-Host "Saved:"
Write-Host "  $rawLogPath"
Write-Host "  $csvPath"
Write-Host "  $mdPath"
