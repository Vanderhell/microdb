# SPDX-License-Identifier: MIT
param(
    [string]$Port = "COM17",
    [int]$Baud = 115200,
    [int]$OpenTimeoutSec = 20,
    [int]$RunTimeoutSec = 180,
    [string]$CommandScript = "resetdb;run;metrics",
    [string]$LogPath = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Read-UntilPattern {
    param(
        [Parameter(Mandatory = $true)][System.IO.Ports.SerialPort]$Serial,
        [Parameter(Mandatory = $true)][string]$Pattern,
        [Parameter(Mandatory = $true)][int]$TimeoutSec,
        [ref]$Buffer
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $chunk = $Serial.ReadExisting()
            if (-not [string]::IsNullOrEmpty($chunk)) {
                $Buffer.Value += $chunk
                Write-Host -NoNewline $chunk
                if ($Buffer.Value -match [regex]::Escape($Pattern)) {
                    return $true
                }
            }
        } catch {
        }
        Start-Sleep -Milliseconds 50
    }
    return $false
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
                Write-Host -NoNewline $chunk
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
    Write-Host ""
    Write-Host ">>> $Cmd"
    $Serial.WriteLine($Cmd)
}

$serial = New-Object System.IO.Ports.SerialPort $Port, $Baud, ([System.IO.Ports.Parity]::None), 8, ([System.IO.Ports.StopBits]::One)
$serial.NewLine = "`n"
$serial.ReadTimeout = 100
$serial.WriteTimeout = 1000
$serial.DtrEnable = $false
$serial.RtsEnable = $false

$fullLog = ""
$ok = $false
$commands = @()
foreach ($part in ($CommandScript -split ';')) {
    $cmd = $part.Trim()
    if (-not [string]::IsNullOrWhiteSpace($cmd)) {
        $commands += $cmd
    }
}

try {
    Write-Host "Opening $Port @ $Baud..."
    $serial.Open()
    Start-Sleep -Milliseconds 800

    # Wake prompt if already running.
    $serial.WriteLine("")

    $buf = ""
    $ready = Read-UntilPattern -Serial $serial -Pattern "loxdb-bench>" -TimeoutSec $OpenTimeoutSec -Buffer ([ref]$buf)
    $fullLog += $buf
    if (-not $ready) {
        throw "Prompt 'loxdb-bench>' not detected on $Port within $OpenTimeoutSec s."
    }

    foreach ($cmd in $commands) {
        Write-Cmd -Serial $serial -Cmd $cmd
        $buf = ""
        if ($cmd -eq "run" -or $cmd -eq "run_det" -or $cmd -eq "run_det_paced") {
            $matched = ""
            $runDone = Read-UntilAnyPattern -Serial $serial -Patterns @("=== loxdb ESP32-S3 benchmark end ===", "loxdb-bench>") -TimeoutSec $RunTimeoutSec -Buffer ([ref]$buf) -MatchedPattern ([ref]$matched)
            $fullLog += $buf
            if (-not $runDone) {
                throw "Benchmark command '$cmd' did not finish within $RunTimeoutSec s."
            }
            if ($matched -eq "loxdb-bench>" -and $buf -notmatch [regex]::Escape("=== loxdb ESP32-S3 benchmark end ===")) {
                throw "Benchmark command '$cmd' returned to prompt without benchmark end marker."
            }
            if ($buf -notmatch [regex]::Escape("loxdb-bench>")) {
                $buf = ""
                $promptBack = Read-UntilPattern -Serial $serial -Pattern "loxdb-bench>" -TimeoutSec 20 -Buffer ([ref]$buf)
                $fullLog += $buf
                if (-not $promptBack) {
                    throw "Prompt not returned after '$cmd'."
                }
            }
        } else {
            $okBack = Read-UntilPattern -Serial $serial -Pattern "loxdb-bench>" -TimeoutSec 30 -Buffer ([ref]$buf)
            $fullLog += $buf
            if (-not $okBack) {
                throw "Command '$cmd' did not return to prompt."
            }
        }
    }

    $failedChecks = @()
    foreach ($line in ($fullLog -split "`r?`n")) {
        if ($line -match "^\[CHECK\].*FAIL") {
            $failedChecks += $line
        }
    }

    if ($failedChecks.Count -gt 0) {
        Write-Host ""
        Write-Host "FAILED checks:"
        $failedChecks | ForEach-Object { Write-Host "  $_" }
        exit 2
    }

    if ($fullLog -notmatch "\[CHECK\] KV benchmark: PASS" -or
        $fullLog -notmatch "\[CHECK\] TS benchmark: PASS" -or
        $fullLog -notmatch "\[CHECK\] REL benchmark: PASS" -or
        $fullLog -notmatch "\[CHECK\] WAL compact: PASS" -or
        $fullLog -notmatch "\[CHECK\] Migration callback: PASS" -or
        $fullLog -notmatch "\[CHECK\] TXN commit/rollback: PASS") {
        throw "Not all expected PASS checks were found."
    }

    $ok = $true

    if (-not [string]::IsNullOrWhiteSpace($LogPath)) {
        [System.IO.File]::WriteAllText($LogPath, $fullLog)
        Write-Host "Saved log: $LogPath"
    }
}
finally {
    if (-not [string]::IsNullOrWhiteSpace($LogPath)) {
        [System.IO.File]::WriteAllText($LogPath, $fullLog)
        Write-Host "Saved log: $LogPath"
    }
    if ($serial.IsOpen) {
        $serial.Close()
    }
}

if ($ok) {
    Write-Host ""
    Write-Host "loxdb benchmark on ${Port}: PASS"
    exit 0
}

exit 1
