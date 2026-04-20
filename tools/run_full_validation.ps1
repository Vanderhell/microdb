# SPDX-License-Identifier: MIT
param(
    [string]$BuildDir = "build",
    [string]$Config = "Debug",
    [switch]$SkipBuild,
    [switch]$SkipDesktop,
    [switch]$SkipEsp32,
    [string]$EspPort = "COM17",
    [int]$EspBaud = 115200,
    [int]$EspRunTimeoutSec = 420
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$resultsDir = Join-Path $root "docs\results"
New-Item -ItemType Directory -Force -Path $resultsDir | Out-Null
$ts = Get-Date -Format "yyyyMMdd_HHmmss"

function Run-Cmd {
    param(
        [Parameter(Mandatory = $true)][string]$Exe,
        [string[]]$Args = @(),
        [string]$OutFile = ""
    )
    Write-Host ">>> $Exe $($Args -join ' ')"
    $output = & $Exe @Args 2>&1
    if ($LASTEXITCODE -ne 0) {
        if ($OutFile -ne "") { $output | Out-File -Encoding utf8 $OutFile }
        throw "Command failed ($LASTEXITCODE): $Exe $($Args -join ' ')"
    }
    if ($OutFile -ne "") { $output | Out-File -Encoding utf8 $OutFile }
    return $output
}

function Parse-SoakCsvLine {
    param([string[]]$Lines)
    foreach ($line in $Lines) {
        if ($line -match '^(deterministic|balanced|stress),') { return $line }
    }
    return ""
}

if (-not $SkipBuild) {
    Run-Cmd -Exe "cmake" -Args @("-S", ".", "-B", $BuildDir) | Out-Null
    Run-Cmd -Exe "cmake" -Args @("--build", $BuildDir, "--config", $Config, "--target", "worstcase_matrix_runner", "soak_runner", "test_fail_code_contract") | Out-Null
}

$summary = @()
$summary += "# Full Validation Summary ($ts)"
$summary += ""

if (-not $SkipDesktop) {
    $profiles = @("deterministic", "balanced", "stress")
    $matrixRows = @()
    $soakRows = @()

    foreach ($p in $profiles) {
        $matrixCsv = Join-Path $resultsDir ("worstcase_matrix_{0}_{1}.csv" -f $p, $ts)
        $soakCsv = Join-Path $resultsDir ("soak_{0}_{1}.csv" -f $p, $ts)

        $matrixOut = Run-Cmd -Exe (Join-Path $root "$BuildDir\$Config\worstcase_matrix_runner.exe") -Args @("--profile", $p) -OutFile $matrixCsv
        $soakOut = Run-Cmd -Exe (Join-Path $root "$BuildDir\$Config\soak_runner.exe") -Args @("--profile", $p) -OutFile $soakCsv

        $matrixData = Import-Csv $matrixCsv
        $matrixRows += $matrixData

        $soakLine = Parse-SoakCsvLine -Lines $soakOut
        if ([string]::IsNullOrWhiteSpace($soakLine)) {
            throw "Soak CSV line not found for profile '$p'"
        }
        $tmp = Join-Path $resultsDir ("soak_parse_tmp_{0}_{1}.csv" -f $p, $ts)
        @(
            "profile,ops,max_kv_put_us,max_kv_del_us,max_ts_insert_us,max_rel_insert_us,max_rel_del_us,max_compact_us,max_reopen_us,spikes_gt_1ms,spikes_gt_5ms,fail_count,slo_pass"
            $soakLine
        ) | Out-File -Encoding utf8 $tmp
        $soakRows += (Import-Csv $tmp)
        Remove-Item $tmp -Force
    }

    $summary += "## Desktop Verdicts"
    foreach ($p in $profiles) {
        $pm = $matrixRows | Where-Object { $_.profile -eq $p }
        $ps = $soakRows | Where-Object { $_.profile -eq $p } | Select-Object -First 1

        $maxKv = ($pm | Measure-Object -Property max_kv_put_us -Maximum).Maximum
        $maxTs = ($pm | Measure-Object -Property max_ts_insert_us -Maximum).Maximum
        $maxRelIns = ($pm | Measure-Object -Property max_rel_insert_us -Maximum).Maximum
        $maxRelDel = ($pm | Measure-Object -Property max_rel_delete_us -Maximum).Maximum
        $maxTxn = ($pm | Measure-Object -Property max_txn_commit_us -Maximum).Maximum
        $maxComp = ($pm | Measure-Object -Property max_compact_us -Maximum).Maximum
        $maxReopen = ($pm | Measure-Object -Property max_reopen_us -Maximum).Maximum
        $spikes = ($pm | Measure-Object -Property spikes_gt_5ms -Sum).Sum
        $fails = ($pm | Measure-Object -Property fail_count -Sum).Sum
        $matrixPass = ((@($pm | Where-Object { $_.slo_pass -eq "0" }).Count) -eq 0)
        $soakPass = ((@($ps)[-1].slo_pass) -eq "1")
        $overall = if ($matrixPass -and $soakPass) { "PASS" } else { "FAIL" }

        $summary += "- profile=$p verdict=$overall matrix_slo=$(if($matrixPass){'PASS'}else{'FAIL'}) soak_slo=$(if($soakPass){'PASS'}else{'FAIL'}) max_kv_put=$maxKv max_ts_insert=$maxTs max_rel_insert=$maxRelIns max_rel_delete=$maxRelDel max_txn_commit=$maxTxn max_compact=$maxComp max_reopen=$maxReopen spikes_gt_5ms=$spikes fail_count=$fails"
    }
    $summary += ""
}

if (-not $SkipEsp32) {
    $summary += "## ESP32 Runs"
    $espScript = Join-Path $root "bench\microdb_esp32_s3_bench\run_bench.ps1"
    $espProfiles = @(
        @{ Name = "deterministic"; Commands = @("run_det", "metrics") },
        @{ Name = "balanced"; Commands = @("profile balanced", "run", "metrics") },
        @{ Name = "stress"; Commands = @("profile stress", "run", "metrics") }
    )

    foreach ($ep in $espProfiles) {
        $logFile = Join-Path $resultsDir ("esp32_{0}_{1}.log" -f $ep.Name, $ts)
        try {
            $commandScript = ($ep.Commands -join ";")
            $espArgs = @("-ExecutionPolicy", "Bypass", "-File", $espScript, "-Port", $EspPort, "-Baud", "$EspBaud", "-RunTimeoutSec", "$EspRunTimeoutSec", "-CommandScript", $commandScript, "-LogPath", $logFile)
            Run-Cmd -Exe "powershell" -Args $espArgs | Out-Null
            $summary += "- profile=$($ep.Name) verdict=PASS log=$logFile"
        } catch {
            $summary += "- profile=$($ep.Name) verdict=FAIL reason=$($_.Exception.Message) log=$logFile"
        }
    }
    $summary += ""
}

$summaryFile = Join-Path $resultsDir ("validation_summary_{0}.md" -f $ts)
$summary | Out-File -Encoding utf8 $summaryFile
Write-Host ""
Write-Host "Saved summary: $summaryFile"

$trendScript = Join-Path $root "scripts\generate-results-trend-dashboard.ps1"
if (Test-Path $trendScript) {
    try {
        & $trendScript -ResultsDir $resultsDir -TopRuns 20 -OutputMarkdown (Join-Path $resultsDir "trend_dashboard.md") | Out-Null
        Write-Host "Updated trend dashboard: $(Join-Path $resultsDir "trend_dashboard.md")"
    } catch {
        Write-Warning "Trend dashboard generation failed: $($_.Exception.Message)"
    }
}
